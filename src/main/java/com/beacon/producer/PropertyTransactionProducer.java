package com.beacon.producer;

import com.beacon.model.PropertyTransaction;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Beacon Inc. — Property Transaction Producer
 *
 * Continuously generates randomized Philippine real estate transactions
 * and sends them to Kafka every second.
 *
 * Key   = parcelId  (ensures partition affinity per property)
 * Value = JSON of PropertyTransaction
 *
 * Run this first, then start PropertyTransactionConsumer in another terminal.
 */
public class PropertyTransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(PropertyTransactionProducer.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    // private static final String BOOTSTRAP_SERVERS = "broker-1:29092";

    private static final String TOPIC_NAME = "beacon-ph-transactions-topic";

    // ----------------------------------------------------------------
    // Philippine real estate data pool
    // ----------------------------------------------------------------

    private static final String[] AREAS = {
            "BGC Taguig", "Makati CBD", "Ortigas Pasig", "Quezon City",
            "Alabang Muntinlupa", "Mandaluyong", "Pasay", "Cebu City",
            "Davao City", "Clark Pampanga", "Iloilo City", "Bacolod",
            "Cagayan de Oro", "San Juan Metro Manila", "Paranaque",
            "Las Pinas", "Caloocan", "Marikina", "Muntinlupa City",
            "Taguig City", "Antipolo Rizal", "Cainta Rizal"
    };

    private static final String[] PARCEL_IDS = {
            "TCT-BGC-C204",  "TCT-MKT-L017",  "TCT-QC-R091",
            "TCT-PSG-COM33", "TCT-ALB-R012",  "TCT-CEB-L044",
            "TCT-DAV-C118",  "TCT-PSY-COM07", "TCT-MND-C399",
            "TCT-CLK-R055",  "TCT-ILO-R022",  "TCT-BAC-C011"
    };

    // {buyer, seller} pairs
    private static final String[][] PARTIES = {
            {"Juan dela Cruz",       "Ayala Land Inc."},
            {"Maria Santos",         "Registry of Deeds Manila"},
            {"Jose Reyes",           "SM Prime Holdings"},
            {"Ana Gonzales",         "Megaworld Corporation"},
            {"Ramon Villanueva",     "Robinsons Land Corp"},
            {"Ligaya Mendoza",       "Federal Land Inc."},
            {"Eduardo Castillo",     "Vista Land & Lifescapes"},
            {"Rosario Bautista",     "Filinvest Land Inc."},
            {"Carlos Flores",        "DMCI Homes"},
            {"Teresa Aquino",        "Rockwell Land Corp"},
    };

    private static final PropertyTransaction.TransactionType[] TX_TYPES =
            PropertyTransaction.TransactionType.values();

    private static final PropertyTransaction.PropertyType[] PROP_TYPES =
            PropertyTransaction.PropertyType.values();

    // Amount range: PHP 1M – PHP 10M (rounded to nearest 10k)
    private static final double MIN_AMOUNT = 1_000_000;
    private static final double MAX_AMOUNT = 10_000_000;

    // ----------------------------------------------------------------

    public static void main(String[] args) throws InterruptedException {

        // Producer config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                  "all");
        props.put(ProducerConfig.RETRIES_CONFIG,               3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,    true);
        props.put(ProducerConfig.LINGER_MS_CONFIG,             5);

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║   Beacon Inc. — Property Transaction Producer        ║");
        logger.info("║   Topic  : " + TOPIC_NAME + "             ║");
        logger.info("║   Broker : " + BOOTSTRAP_SERVERS + "                      ║");
        logger.info("║   Press Ctrl+C to stop                               ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer...");
            producer.flush();
            producer.close();
            logger.info("Producer closed cleanly.");
        }));

        int count = 0;

        while (true) {
            PropertyTransaction tx = buildRandomTransaction();

            String key   = tx.getParcelId();   // partition key
            String value = tx.toJson();         // full JSON payload

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, key, value);

            logger.info("Sending transaction: " + tx);

            producer.send(record, (metadata, exception) -> {
                if (metadata != null) {
                    logger.info("-----------------------------------");
                    logger.info("Key (Parcel)  : " + record.key());
                    logger.info("Topic         : " + metadata.topic());
                    logger.info("Partition     : " + metadata.partition());
                    logger.info("Offset        : " + metadata.offset());
                    logger.info("Timestamp     : " + metadata.timestamp());
                } else if (exception != null) {
                    logger.error("Error sending message to Kafka", exception);
                }
            });

            producer.flush();

            try {
                Thread.sleep(1000); // one message per second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // ----------------------------------------------------------------
    // Build a random but realistic transaction
    // ----------------------------------------------------------------

    private static PropertyTransaction buildRandomTransaction() {
        int propIdx   = rand(PROP_TYPES.length);
        int txIdx     = rand(TX_TYPES.length);
        int partyIdx  = rand(PARTIES.length);
        int parcelIdx = rand(PARCEL_IDS.length);
        int areaIdx   = rand(AREAS.length);

        PropertyTransaction.TransactionType txType   = TX_TYPES[txIdx];
        PropertyTransaction.PropertyType    propType = PROP_TYPES[propIdx];

        // TITLE_REGISTRATION has no monetary amount
        double amount = 0;
        if (txType != PropertyTransaction.TransactionType.TITLE_REGISTRATION) {
            amount = Math.floor(
                    (Math.random() * (MAX_AMOUNT - MIN_AMOUNT) + MIN_AMOUNT) / 10_000
            ) * 10_000;
        }

        // Occasionally inject a bad record to demonstrate validation (1 in 10)
        if (Math.random() < 0.10) {
            return buildInvalidTransaction();
        }

        return PropertyTransaction.of(
                PARCEL_IDS[parcelIdx],
                txType,
                propType,
                PARTIES[partyIdx][0],
                PARTIES[partyIdx][1],
                amount,
                AREAS[areaIdx]
        );
    }

    /**
     * Deliberately invalid transaction to demonstrate consumer-side validation.
     * Rotates through different failure scenarios.
     */
    private static int invalidCount = 0;
    private static PropertyTransaction buildInvalidTransaction() {
        invalidCount++;
        return switch (invalidCount % 4) {
            case 0 -> // R4: PURCHASE with zero amount
                    PropertyTransaction.of("TCT-BGC-C204",
                            PropertyTransaction.TransactionType.PURCHASE,
                            PropertyTransaction.PropertyType.CONDO,
                            "Juan dela Cruz", "Ayala Land Inc.", 0, "BGC Taguig");
            case 1 -> // R2: Bad parcel ID format (not TCT-)
                    PropertyTransaction.of("PARCEL-WRONG-001",
                            PropertyTransaction.TransactionType.TRANSFER,
                            PropertyTransaction.PropertyType.LAND,
                            "Maria Santos", "SM Prime Holdings", 3_500_000, "Makati CBD");
            case 2 -> { // R5: Missing buyer name
                PropertyTransaction t = PropertyTransaction.of("TCT-QC-R091",
                        PropertyTransaction.TransactionType.MORTGAGE,
                        PropertyTransaction.PropertyType.RESIDENTIAL,
                        "", "Robinsons Land Corp", 8_000_000, "Quezon City");
                yield t;
            }
            default -> // W1: Extremely high amount (warning only, still valid)
                    PropertyTransaction.of("TCT-MKT-L017",
                            PropertyTransaction.TransactionType.PURCHASE,
                            PropertyTransaction.PropertyType.COMMERCIAL,
                            "Carlos Flores", "Rockwell Land Corp", 95_000_000, "Makati CBD");
        };
    }

    private static int rand(int bound) {
        return (int) (Math.random() * bound);
    }
}
