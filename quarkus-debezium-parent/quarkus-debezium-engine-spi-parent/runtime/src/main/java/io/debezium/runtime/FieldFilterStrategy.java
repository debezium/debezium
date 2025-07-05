/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import io.debezium.spi.converter.ConvertedField;

public interface FieldFilterStrategy {
    boolean filter(ConvertedField field);
}
