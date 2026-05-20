# Beacon Inc. — Kafka → Validate → PostgreSQL Pipeline
### Property Transaction Pipeline Explainer

---

## What We Built

A real-time data pipeline that simulates the **Corelate Platform** — a Philippine real estate system that streams property transactions through Apache Kafka, validates them, and persists clean records into PostgreSQL.

```
Producer (NiFi / App)
      │
      │  JSON event (PropertyTransaction)
      ▼
Apache Kafka
  Topic: beacon-transactions-topic
  Key:   parcelId  (partition affinity)
      │
      ▼
Consumer
  ├── Deserialize JSON
  ├── Validate (8 rules)
  │     ├── VALID   → INSERT into PostgreSQL
  │     └── INVALID → log and skip
      │
      ▼
PostgreSQL
  Table: property_transactions
```

---

## Project Structure

```
src/main/java/com/beacon/
├── model/
│   └── PropertyTransaction.java
├── producer/
│   └── PropertyTransactionProducer.java
├── consumer/
│   └── PropertyTransactionConsumer.java
├── validation/
│   ├── TransactionValidator.java
│   └── ValidationResult.java
└── db/
    └── TransactionRepository.java
```

---

## File by File Explainer

---

### 1. `PropertyTransaction.java` — The Data Model

**What it is:** The Java object that represents one property transaction event. Every message flowing through Kafka is an instance of this class, serialized as JSON.

**Key fields:**

| Field | Example | Purpose |
|---|---|---|
| `transactionId` | `TXN-A3F92B1C` | Unique ID for the transaction |
| `parcelId` | `TCT-BGC-C204` | Land title number (Philippine TCT format) |
| `transactionType` | `PURCHASE` | Type of transaction |
| `propertyType` | `CONDO` | Type of property |
| `buyerName` | `Juan dela Cruz` | Buyer |
| `sellerName` | `Ayala Land Inc.` | Seller |
| `transactionAmount` | `4500000.00` | Amount in PHP |
| `area` | `BGC Taguig` | Location |
| `titleDeedUri` | `s3://beacon-docs/...` | Reference to the actual document in object storage |
| `validationStatus` | `VALID / INVALID` | Set by consumer after validation |

**Key methods:**
- `toJson()` — serializes the object to JSON string (used by producer before sending to Kafka)
- `fromJson(String)` — deserializes JSON back to object (used by consumer after receiving from Kafka)

**Why it matters:** This is the contract between producer and consumer. Both sides agree on this structure. In a real system this would be an Avro schema registered in Schema Registry.

---

### 2. `PropertyTransactionProducer.java` — Sends Events to Kafka

**What it does:** Generates a randomized Philippine real estate transaction every second and publishes it to Kafka as a JSON message.

**Key Kafka config used:**

| Config | Value | Why |
|---|---|---|
| `acks=all` | Wait for all replicas | No data loss |
| `enable.idempotence=true` | Exactly-once delivery | No duplicate messages |
| `retries=3` | Retry on failure | Resilience |
| `linger.ms=5` | Wait 5ms before sending | Better batching |

**Partition key:** `parcelId` is used as the Kafka message key. This means all events for the same property always go to the **same partition**, guaranteeing ordering per parcel.

**Deliberate bad records:** 1 in every 10 messages is intentionally invalid, rotating through 4 failure scenarios:
- PURCHASE with zero amount
- Wrong parcel ID format (not `TCT-*`)
- Missing buyer name
- Extremely high amount (warning only)

This is done to demonstrate validation in action during training.

**Shutdown hook:** `producer.flush()` is called on Ctrl+C to ensure no in-flight messages are lost.

---

### 3. `PropertyTransactionConsumer.java` — Receives and Processes Events

**What it does:** Continuously polls Kafka for new messages, deserializes them, validates each one, and writes valid transactions to PostgreSQL.

**Consumer group:** `beacon-validation-postgres-group`
All instances of this consumer share the same group, meaning Kafka distributes partitions across them. If you run two consumers in the same group, they split the load.

**Offset strategy:** `ENABLE_AUTO_COMMIT=false` — offsets are committed **manually**, only after the PostgreSQL write succeeds. This is critical:

```
poll() → validate → INSERT to PostgreSQL → commitSync()
                                           ↑
                         only commit after DB write succeeds
                         if DB write fails → message is retried
```

This gives **at-least-once delivery** — a message may be processed more than once but never lost.

**Processing flow per message:**
```
1. Deserialize JSON → PropertyTransaction object
2. Run TransactionValidator
3a. VALID   → set status=PROCESSED → INSERT to PostgreSQL → commit offset
3b. INVALID → log rejection details → skip → commit offset
```

---

### 4. `TransactionValidator.java` — Business Rules Engine

**What it does:** Runs 8 hard rules and 3 soft rules against each transaction. Hard rule failures reject the record. Soft rule failures are warnings — the record still goes to PostgreSQL but gets flagged.

**Hard rules (INVALID if any fail):**

| Rule | Check |
|---|---|
| R1 | Transaction ID must exist |
| R2 | Parcel ID must follow `TCT-*` format (Transfer Certificate of Title) |
| R3 | Amount must not be negative |
| R4 | PURCHASE / TRANSFER / MORTGAGE / LEASE must have amount > 0 |
| R5 | Buyer name must be present |
| R6 | Seller name must be present |
| R7 | Transaction type must be a known value |
| R8 | Buyer and seller cannot be the same person |

**Soft rules (WARNING only):**

| Rule | Check |
|---|---|
| W1 | Amount > PHP 50,000,000 — flagged for manual review |
| W2 | Title deed URI missing |
| W3 | Area not in known Philippine locations list |

**Why TCT format matters:** In the Philippines, all land titles are issued as Transfer Certificates of Title (TCT) by the Registry of Deeds. A parcel ID that doesn't follow this format indicates bad or test data.

---

### 5. `ValidationResult.java` — Validation Outcome Holder

**What it is:** A simple container returned by `TransactionValidator`. Holds whether the transaction passed, plus lists of errors and warnings.

```java
ValidationResult result = validator.validate(tx);

if (result.isValid()) {
    // write to PostgreSQL
} else {
    // log result.getErrors() and skip
}
```

Kept separate from the validator so the consumer doesn't need to know validation logic — it just checks `isValid()`.

---

### 6. `TransactionRepository.java` — Writes to PostgreSQL

**What it does:** Plain JDBC connection to PostgreSQL. Creates the table if it doesn't exist, then inserts validated transactions.

**Table created automatically:**
```sql
CREATE TABLE IF NOT EXISTS property_transactions (
    transaction_id      VARCHAR(50)    PRIMARY KEY,
    parcel_id           VARCHAR(50)    NOT NULL,
    transaction_type    VARCHAR(30)    NOT NULL,
    property_type       VARCHAR(30)    NOT NULL,
    area                VARCHAR(100),
    city                VARCHAR(100),
    buyer_name          VARCHAR(200)   NOT NULL,
    seller_name         VARCHAR(200)   NOT NULL,
    transaction_amount  NUMERIC(15,2)  NOT NULL,
    currency            VARCHAR(10)    DEFAULT 'PHP',
    title_deed_uri      VARCHAR(500),
    status              VARCHAR(30)    DEFAULT 'PENDING',
    validation_status   VARCHAR(20)    DEFAULT 'VALID',
    transaction_date    TIMESTAMP      NOT NULL,
    inserted_at         TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);
```

**Idempotent insert:** Uses `ON CONFLICT (transaction_id) DO NOTHING` — if the same transaction is processed twice (due to at-least-once delivery), the second insert is silently ignored. No duplicates in the database.

**Connection config:**
```java
DB_URL  = "jdbc:postgresql://localhost:5432/beacon_retail?sslmode=disable"
DB_USER = "postgres"
DB_PASSWORD = "admin"
```

---

## How to Run

### Step 1 — Start Kafka (Docker)
```bash
docker compose up -d
```

### Step 2 — Start the consumer first (Terminal 1)
```bash
# Run PropertyTransactionConsumer
# It will wait for messages and write to PostgreSQL
```

### Step 3 — Start the producer (Terminal 2)
```bash
# Run PropertyTransactionProducer
# Sends one transaction per second
# Press Ctrl+C to stop
```

### Step 4 — Verify PostgreSQL
```sql
SELECT transaction_id, parcel_id, transaction_type,
       buyer_name, transaction_amount, validation_status
FROM property_transactions
ORDER BY inserted_at DESC
LIMIT 10;
```

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Producer with idempotence | `PropertyTransactionProducer` |
| Message key for partition affinity | `parcelId` as Kafka key |
| JSON serialization over Kafka | `PropertyTransaction.toJson() / fromJson()` |
| Manual offset commit | `PropertyTransactionConsumer` — commit after DB write |
| At-least-once delivery | Manual commit + idempotent DB insert |
| Consumer group | `beacon-validation-postgres-group` |
| Business rule validation | `TransactionValidator` — 8 rules |
| Plain JDBC to PostgreSQL | `TransactionRepository` |
| Graceful shutdown | Shutdown hooks in both producer and consumer |

---

## Common Issues & Fixes

| Error | Cause | Fix |
|---|---|---|
| `NoSuchMethodError: BufferRecycler.releaseToPool()` | Jackson version mismatch (`jackson-core` 2.15 vs `jackson-databind` 2.17) | Remove hardcoded version from `jackson-core` dependency — let `dependencyManagement` control it |
| `pg_hba.conf` auth error | PostgreSQL rejecting connection from network IP | Add `?sslmode=disable` to JDBC URL |
| `host.docker.internal` resolving to network IP | Consumer running in IntelliJ (not Docker) | Use `localhost:5432` instead of `host.docker.internal` |

---

## Connection to Corelate Architecture

This pipeline represents one slice of the full Corelate platform:

```
PostgreSQL (source)
    → NiFi (Adapter Layer)     ← PropertyTransactionProducer simulates this
    → Kafka                    ← beacon-transactions-topic
    → Spark + Validation       ← PropertyTransactionConsumer simulates this
    → Hudi (lakehouse)         ← next step: replace PostgreSQL sink with Hudi
    → Yugabyte (live queries)
```

The PostgreSQL **sink** here is a simplification for training. In the actual Corelate platform, validated transactions would be written to **Apache Hudi** first, then synced to Yugabyte for live queries.