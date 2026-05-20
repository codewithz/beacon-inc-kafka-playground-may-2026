package com.beacon.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Beacon Inc. — Property Transaction
 *
 * Represents a single property transaction event.
 * Serialized to JSON for Kafka transport.
 * Deserialized by consumer and written to PostgreSQL.
 */
public class PropertyTransaction {

    public enum TransactionType {
        PURCHASE, TRANSFER, MORTGAGE, TITLE_REGISTRATION, LEASE
    }

    public enum PropertyType {
        CONDO, LAND, RESIDENTIAL, COMMERCIAL
    }

    public enum ValidationStatus {
        PENDING, VALID, INVALID
    }

    // --- Identity ---
    private String transactionId;
    private String parcelId;

    // --- Classification ---
    private TransactionType transactionType;
    private PropertyType propertyType;

    // --- Location ---
    private String area;
    private String city;

    // --- Parties ---
    private String buyerName;
    private String sellerName;

    // --- Financial ---
    private double transactionAmount;
    private String currency;

    // --- Document reference ---
    private String titleDeedUri;

    // --- Status ---
    private String status;
    private ValidationStatus validationStatus;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime transactionDate;

    // ----------------------------------------------------------------
    // Constructors
    // ----------------------------------------------------------------

    public PropertyTransaction() {}

    public static PropertyTransaction of(
            String parcelId,
            TransactionType txType,
            PropertyType propType,
            String buyerName,
            String sellerName,
            double amount,
            String area) {

        PropertyTransaction t = new PropertyTransaction();
        t.transactionId    = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        t.parcelId         = parcelId;
        t.transactionType  = txType;
        t.propertyType     = propType;
        t.buyerName        = buyerName;
        t.sellerName       = sellerName;
        t.transactionAmount = amount;
        t.currency         = "PHP";
        t.area             = area;
        t.city             = "Metro Manila";
        t.titleDeedUri     = "s3://beacon-docs/titles/" + parcelId + "/deed.pdf";
        t.status           = "PENDING";
        t.validationStatus = ValidationStatus.PENDING;
        t.transactionDate  = LocalDateTime.now();
        return t;
    }

    // ----------------------------------------------------------------
    // JSON helpers
    // ----------------------------------------------------------------

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize transaction", e);
        }
    }

    public static PropertyTransaction fromJson(String json) {
        try {
            return MAPPER.readValue(json, PropertyTransaction.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize transaction: " + json, e);
        }
    }

    // ----------------------------------------------------------------
    // Getters & Setters
    // ----------------------------------------------------------------

    public String getTransactionId()                   { return transactionId; }
    public void setTransactionId(String v)             { this.transactionId = v; }

    public String getParcelId()                        { return parcelId; }
    public void setParcelId(String v)                  { this.parcelId = v; }

    public TransactionType getTransactionType()        { return transactionType; }
    public void setTransactionType(TransactionType v)  { this.transactionType = v; }

    public PropertyType getPropertyType()              { return propertyType; }
    public void setPropertyType(PropertyType v)        { this.propertyType = v; }

    public String getArea()                            { return area; }
    public void setArea(String v)                      { this.area = v; }

    public String getCity()                            { return city; }
    public void setCity(String v)                      { this.city = v; }

    public String getBuyerName()                       { return buyerName; }
    public void setBuyerName(String v)                 { this.buyerName = v; }

    public String getSellerName()                      { return sellerName; }
    public void setSellerName(String v)                { this.sellerName = v; }

    public double getTransactionAmount()               { return transactionAmount; }
    public void setTransactionAmount(double v)         { this.transactionAmount = v; }

    public String getCurrency()                        { return currency; }
    public void setCurrency(String v)                  { this.currency = v; }

    public String getTitleDeedUri()                    { return titleDeedUri; }
    public void setTitleDeedUri(String v)              { this.titleDeedUri = v; }

    public String getStatus()                          { return status; }
    public void setStatus(String v)                    { this.status = v; }

    public ValidationStatus getValidationStatus()              { return validationStatus; }
    public void setValidationStatus(ValidationStatus v)        { this.validationStatus = v; }

    public LocalDateTime getTransactionDate()          { return transactionDate; }
    public void setTransactionDate(LocalDateTime v)    { this.transactionDate = v; }

    @Override
    public String toString() {
        return String.format("[%s] %s | %s | %s → %s | PHP %,.0f | %s",
                transactionId, transactionType, parcelId,
                sellerName, buyerName, transactionAmount, validationStatus);
    }
}