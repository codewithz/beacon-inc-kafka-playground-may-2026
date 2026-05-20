package com.beacon.validation;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds the outcome of validating a PropertyTransaction.
 * Consumer runs these checks before writing to PostgreSQL.
 */
public class ValidationResult {

    private final boolean valid;
    private final List<String> errors;
    private final List<String> warnings;

    private ValidationResult(boolean valid, List<String> errors, List<String> warnings) {
        this.valid    = valid;
        this.errors   = errors;
        this.warnings = warnings;
    }

    public static ValidationResult pass(List<String> warnings) {
        return new ValidationResult(true, List.of(), warnings);
    }

    public static ValidationResult fail(List<String> errors, List<String> warnings) {
        return new ValidationResult(false, errors, warnings);
    }

    public boolean isValid()           { return valid; }
    public List<String> getErrors()    { return errors; }
    public List<String> getWarnings()  { return warnings; }

    public void print(String txnId) {
        if (valid) {
            System.out.println("   ✅ VALID   → " + txnId);
            warnings.forEach(w -> System.out.println("   ⚠️  WARN    → " + w));
        } else {
            System.out.println("   ❌ INVALID → " + txnId);
            errors.forEach(e   -> System.out.println("   ✗  ERROR   → " + e));
            warnings.forEach(w -> System.out.println("   ⚠️  WARN    → " + w));
        }
    }
}