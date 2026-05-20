package com.beacon.db;

import com.beacon.model.PropertyTransaction;

import java.sql.*;

/**
 * Beacon Inc. — Transaction Repository
 *
 * Plain JDBC — no framework, no ORM.
 * Connects to PostgreSQL and inserts validated transactions.
 *
 * Table: property_transactions
 * Schema: see createTableIfNotExists() below
 *
 * Connection config: update DB_URL, DB_USER, DB_PASSWORD to match your setup.
 * Default assumes: Docker postgres container on localhost:5432
 */
public class TransactionRepository {

    // ----------------------------------------------------------------
    // Config — update these for your environment
    // ----------------------------------------------------------------
    private static final String DB_URL =
            "jdbc:postgresql://localhost:5432/beacon_retail?sslmode=disable";
    private static final String DB_USER     = "postgres";
    private static final String DB_PASSWORD = "admin";
    // ----------------------------------------------------------------

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS property_transactions (
                transaction_id      VARCHAR(50)     PRIMARY KEY,
                parcel_id           VARCHAR(50)     NOT NULL,
                transaction_type    VARCHAR(30)     NOT NULL,
                property_type       VARCHAR(30)     NOT NULL,
                area                VARCHAR(100),
                city                VARCHAR(100),
                buyer_name          VARCHAR(200)    NOT NULL,
                seller_name         VARCHAR(200)    NOT NULL,
                transaction_amount  NUMERIC(15, 2)  NOT NULL,
                currency            VARCHAR(10)     DEFAULT 'PHP',
                title_deed_uri      VARCHAR(500),
                status              VARCHAR(30)     DEFAULT 'PENDING',
                validation_status   VARCHAR(20)     DEFAULT 'VALID',
                transaction_date    TIMESTAMP       NOT NULL,
                inserted_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
            );
            """;

    private static final String INSERT_SQL = """
            INSERT INTO property_transactions (
                transaction_id, parcel_id, transaction_type, property_type,
                area, city, buyer_name, seller_name,
                transaction_amount, currency, title_deed_uri,
                status, validation_status, transaction_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO NOTHING;
            """;

    private final Connection connection;

    public TransactionRepository() throws SQLException {
        this.connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        createTableIfNotExists();
        System.out.println("✅ Connected to PostgreSQL: " + DB_URL);
        System.out.println("✅ Table 'property_transactions' ready");
    }

    private void createTableIfNotExists() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
        }
    }

    /**
     * Insert a validated transaction.
     * Returns true if inserted, false if transaction_id already exists (idempotent).
     */
    public boolean insert(PropertyTransaction tx) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            ps.setString(1,  tx.getTransactionId());
            ps.setString(2,  tx.getParcelId());
            ps.setString(3,  tx.getTransactionType().name());
            ps.setString(4,  tx.getPropertyType().name());
            ps.setString(5,  tx.getArea());
            ps.setString(6,  tx.getCity());
            ps.setString(7,  tx.getBuyerName());
            ps.setString(8,  tx.getSellerName());
            ps.setDouble(9,  tx.getTransactionAmount());
            ps.setString(10, tx.getCurrency());
            ps.setString(11, tx.getTitleDeedUri());
            ps.setString(12, tx.getStatus());
            ps.setString(13, tx.getValidationStatus().name());
            ps.setTimestamp(14, Timestamp.valueOf(tx.getTransactionDate()));

            int rows = ps.executeUpdate();
            return rows > 0;
        }
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("👋 PostgreSQL connection closed.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }
}