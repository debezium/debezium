/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * A {@link ColumnMapper} implementation that ensures that string values longer than a specified length will be truncated.
 * 
 * @author Randall Hauch
 */
public class TruncateStrings implements ColumnMapper {

    private final TruncatingValueConverter converter;

    /**
     * Create a {@link ColumnMapper} that truncates string values to a maximum length.
     * 
     * @param maxLength the maximum number of characters allowed in values
     * @throws IllegalArgumentException if the {@code maxLength} is not positive
     */
    public TruncateStrings(int maxLength) {
        if (maxLength <= 0) throw new IllegalArgumentException("Maximum length must be positive");
        this.converter = new TruncatingValueConverter(maxLength);
    }

    @Override
    public ValueConverter create(Column column) {
        return converter;
    }

    @Override
    public void alterFieldSchema(SchemaBuilder schemaBuilder) {
        Configuration params = Configuration.from(schemaBuilder.parameters());
        Integer length = params.getInteger("length");
        if ( length != null && converter.maxLength < length ) {
            // Overwrite the parameter ...
            schemaBuilder.parameter("length",Integer.toString(converter.maxLength));
        }
        Integer maxLength = params.getInteger("maxLength");
        if ( maxLength != null && converter.maxLength < maxLength ) {
            // Overwrite the parameter ...
            schemaBuilder.parameter("maxLength",Integer.toString(converter.maxLength));
        }
        if ( maxLength == null && length == null ) {
            schemaBuilder.parameter("maxLength",Integer.toString(converter.maxLength));
        }
    }

    @Immutable
    protected static final class TruncatingValueConverter implements ValueConverter {
        protected final int maxLength;

        public TruncatingValueConverter(int maxLength) {
            this.maxLength = maxLength;
            assert this.maxLength > 0;
        }

        @Override
        public Object convert(Object value) {
            if (value instanceof String) {
                String str = (String) value;
                if (str.length() > maxLength) return str.substring(0, maxLength);
            }
            return value;
        }
    }

}
