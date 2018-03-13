/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal.converters;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.reactive.converters.AsSourceRecord;
import io.debezium.reactive.spi.AsType;
import io.debezium.reactive.spi.Converter;

/**
 * A no-op converter that returns to {@link SourceRecord}
 *
 * @author Jiri Pechanec
 *
 */
public class NoOpConverter implements Converter<SourceRecord> {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     */
    public void configure(Map<String, ?> configs) {
    }

    /**
     * Not implemented for this converter
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public SourceRecord convertKey(SourceRecord record) {
        throw new UnsupportedOperationException("A key cannot be obtained separately for no-op convertor");
    }

    /**
     * @return the identical record requested to be converted 
     */
    @Override
    public SourceRecord convertValue(SourceRecord record) {
        return record;
    }

    @Override
    public Class<? extends AsType<SourceRecord>> getConvertedType() {
        return AsSourceRecord.class;
    }

}
