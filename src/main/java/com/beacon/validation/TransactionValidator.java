package com.beacon.validation;

import com.beacon.model.PropertyTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Beacon Inc. — Property Transaction Validator
 *
 * Runs business-rule validations on each transaction the consumer receives.
 * Only VALID transactions are written to PostgreSQL.
 * INVALID transactions are logged and skipped (or sent to a dead-letter topic).
 *
 * Rules enforced:
 *   HARD (failures) — transaction rejected if any of these fail
 *     R1: Transaction ID must be present
 *     R2: Parcel ID must be present and follow TCT-* format
 *     R3: Transaction amount must be >= 0
 *     R4: PURCHASE/TRANSFER/MORTGAGE/LEASE must have amount > 0
 *     R5: Buyer name must be present
 *     R6: Seller name must be present
 *     R7: Transaction type must be a known value
 *     R8: Buyer and seller cannot be the same person
 *
 *   SOFT (warnings) — transaction accepted but warning logged
 *     W1: Very high amount (> PHP 50M) — flag for manual review
 *     W2: Title deed URI missing
 *     W3: Area not in known Philippine locations list
 */
public class TransactionValidator {

    private static final double MAX_AMOUNT_WARNING = 50_000_000;

    private static final Set<String> KNOWN_AREAS = Set.of(
            "BGC Taguig", "Makati CBD", "Ortigas Pasig", "Quezon City",
            "Alabang Muntinlupa", "Mandaluyong", "Pasay", "Cebu City",
            "Davao City", "Clark Pampanga", "Iloilo City", "Bacolod",
            "Cagayan de Oro", "Zamboanga City", "General Santos",
            "San Juan Metro Manila", "Paranaque", "Las Pinas",
            "Caloocan", "Valenzuela", "Marikina", "Pasig City",
            "Muntinlupa City", "Taguig City", "Pateros",
            "Antipolo Rizal", "Cainta Rizal", "Taytay Rizal"
    );

    private static final Set<String> AMOUNT_REQUIRED_TYPES = Set.of(
            "PURCHASE", "TRANSFER", "MORTGAGE", "LEASE"
    );

    public ValidationResult validate(PropertyTransaction tx) {
        List<String> errors   = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // ---- Hard rules ------------------------------------------------

        // R1: Transaction ID
        if (isBlank(tx.getTransactionId())) {
            errors.add("R1: Transaction ID is missing");
        }

        // R2: Parcel ID format
        if (isBlank(tx.getParcelId())) {
            errors.add("R2: Parcel ID is missing");
        } else if (!tx.getParcelId().startsWith("TCT-")) {
            errors.add("R2: Parcel ID must follow TCT-* format (Transfer Certificate of Title). Got: " + tx.getParcelId());
        }

        // R3: Amount must not be negative
        if (tx.getTransactionAmount() < 0) {
            errors.add("R3: Transaction amount cannot be negative. Got: " + tx.getTransactionAmount());
        }

        // R4: Monetary transactions must have amount > 0
        if (tx.getTransactionType() != null &&
                AMOUNT_REQUIRED_TYPES.contains(tx.getTransactionType().name()) &&
                tx.getTransactionAmount() == 0) {
            errors.add("R4: " + tx.getTransactionType() + " transaction must have amount > 0");
        }

        // R5: Buyer name
        if (isBlank(tx.getBuyerName())) {
            errors.add("R5: Buyer name is missing");
        }

        // R6: Seller name
        if (isBlank(tx.getSellerName())) {
            errors.add("R6: Seller name is missing");
        }

        // R7: Transaction type
        if (tx.getTransactionType() == null) {
            errors.add("R7: Transaction type is missing or unknown");
        }

        // R8: Buyer != Seller
        if (!isBlank(tx.getBuyerName()) && tx.getBuyerName().equalsIgnoreCase(tx.getSellerName())) {
            errors.add("R8: Buyer and seller cannot be the same person: " + tx.getBuyerName());
        }

        // ---- Soft rules ------------------------------------------------

        // W1: Unusually high amount
        if (tx.getTransactionAmount() > MAX_AMOUNT_WARNING) {
            warnings.add("W1: High-value transaction (PHP " + String.format("%,.0f", tx.getTransactionAmount()) + ") — flagged for manual review");
        }

        // W2: Title deed URI
        if (isBlank(tx.getTitleDeedUri())) {
            warnings.add("W2: Title deed document URI is missing — document not yet uploaded");
        }

        // W3: Known area
        if (!isBlank(tx.getArea()) && !KNOWN_AREAS.contains(tx.getArea())) {
            warnings.add("W3: Area '" + tx.getArea() + "' is not in the known Philippine locations list");
        }

        // ----------------------------------------------------------------

        return errors.isEmpty()
                ? ValidationResult.pass(warnings)
                : ValidationResult.fail(errors, warnings);
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}