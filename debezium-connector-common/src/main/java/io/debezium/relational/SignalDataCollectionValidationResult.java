/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.List;

/**
 * Outcome of {@link SignalDataCollectionValidator#validate}.
 *
 * @param errors problems that must fail configuration
 * @param warnings problems that should be surfaced but not fail configuration
 *
 * @author Debezium Authors
 */
public record SignalDataCollectionValidationResult(List<String> errors, List<String> warnings) {

    public boolean isValid() {
        return errors.isEmpty();
    }
}
