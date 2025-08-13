/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.converter.ConvertedField;

/**
 *
 * {@link FieldFilterStrategy} defines which {@link ConvertedField} can flow to {@link CustomConverter}
 *
 */
@Incubating
public interface FieldFilterStrategy {

    /**
     * @param field {@link ConvertedField} that
     * @return boolean if true {@link ConvertedField} should be applied to {@link CustomConverter}
     */
    boolean filter(ConvertedField field);

    class DefaultFieldFilterStrategy implements FieldFilterStrategy {
        @Override
        public boolean filter(ConvertedField field) {
            return true;
        }
    }
}
