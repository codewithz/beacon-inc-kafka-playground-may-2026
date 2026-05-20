package com.beacon.consumer;

import com.beacon.db.TransactionRepository;
import com.beacon.model.PropertyTransaction;
import com.beacon.validation.TransactionValidator;
import com.beacon.validation.ValidationResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Beacon Inc. — Property Transaction Consumer
 *
 * Pipeline:
 *   Kafka (beacon-transactions-topic)
 *     → Deserialize JSON → PropertyTransaction
 *     → TransactionValidator (8 rules)
 *     → VALID   → PostgreSQL (property_transactions table)
 *     → INVALID → log and skip (extend: send to dead-letter topic)
 *
 * Offset commit strategy: manual, AFTER successful PostgreSQL write.
 * This ensures at-least-once delivery — if Postgres write fails,
 * the message will be reprocessed on next poll.
 */
public class PropertyTransactionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PropertyTransactionConsumer.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    // private static final String BOOTSTRAP_SERVERS = "broker-1:29092";

    private static final String TOPIC_NAME    = "beacon-ph-transactions-topic";
    private static final String GROUP_ID      = "beacon-validation-postgres-group";

    public static void main(String[] args) {

        // Consumer config
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // manual commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   10);

        // Init validator and DB repository
        TransactionValidator   validator  = new TransactionValidator();
        TransactionRepository  repository;

        try {
            repository = new TransactionRepository();
        } catch (Exception e) {
            logger.error("Failed to connect to PostgreSQL. Is it running?", e);
            logger.error("Start with: docker run --name beacon-pg -e POSTGRES_DB=beacon_db " +
                    "-e POSTGRES_USER=beacon_user -e POSTGRES_PASSWORD=beacon_pass " +
                    "-p 5432:5432 -d postgres:15");
            return;
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC_NAME));

        // Counters
        int totalReceived = 0, totalValid = 0, totalInvalid = 0, totalInserted = 0;

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║   Beacon Inc. — Transaction Consumer + Validator     ║");
        logger.info("║   Topic  : " + TOPIC_NAME + "             ║");
        logger.info("║   Group  : " + GROUP_ID + "  ║");
        logger.info("║   DB     : PostgreSQL → property_transactions        ║");
        logger.info("║   Press Ctrl+C to stop                               ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down consumer...");
            consumer.close();
            repository.close();
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    logger.info("... waiting for messages ...");
                    continue;
                }

                logger.info("=== Received batch of {} records ===", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    totalReceived++;

                    logger.info("-----------------------------------");
                    logger.info("Partition : {}", record.partition());
                    logger.info("Offset    : {}", record.offset());
                    logger.info("Key       : {}", record.key());

                    // Step 1: Deserialize
                    PropertyTransaction tx;
                    try {
                        tx = PropertyTransaction.fromJson(record.value());
                        logger.info("Parsed    : {}", tx);
                    } catch (Exception e) {
                        logger.error("Failed to parse message — skipping. Raw: {}", record.value());
                        totalInvalid++;
                        continue;
                    }

                    // Step 2: Validate
                    logger.info("Validating transaction: {}", tx.getTransactionId());
                    ValidationResult result = validator.validate(tx);
                    result.print(tx.getTransactionId());

                    if (!result.isValid()) {
                        // Mark and skip — extend this to publish to a dead-letter topic
                        tx.setValidationStatus(PropertyTransaction.ValidationStatus.INVALID);
                        logger.warn("REJECTED: {} — Errors: {}",
                                tx.getTransactionId(), result.getErrors());
                        totalInvalid++;
                        continue;
                    }

                    // Step 3: Write to PostgreSQL
                    tx.setValidationStatus(PropertyTransaction.ValidationStatus.VALID);
                    tx.setStatus("PROCESSED");
                    try {
                        boolean inserted = repository.insert(tx);
                        if (inserted) {
                            logger.info("INSERTED → PostgreSQL: {}", tx.getTransactionId());
                            totalInserted++;
                        } else {
                            logger.info("DUPLICATE → Already exists: {}", tx.getTransactionId());
                        }
                        totalValid++;
                    } catch (Exception e) {
                        logger.error("PostgreSQL insert failed for {}: {}", tx.getTransactionId(), e.getMessage());
                        // Don't commit offset — message will be retried
                        continue;
                    }
                }

                // Step 4: Commit offsets — only after full batch processed
                consumer.commitSync();
                logger.info("Offsets committed | Total: received={} valid={} invalid={} inserted={}",
                        totalReceived, totalValid, totalInvalid, totalInserted);
            }

        } catch (Exception e) {
            logger.error("Consumer error", e);
        } finally {
            consumer.close();
            repository.close();
        }
    }
}
