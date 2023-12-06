/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.spi;

import io.debezium.relational.ValueConverterProvider;

/**
 * Contract that allows injecting a {@link ValueConverterProvider}.
 *
 * @author Chris Cranford
 */
public interface ValueConverterAware {
    void setValueConverter(ValueConverterProvider valueConverter);
}
