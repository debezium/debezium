/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * A {@link ColumnMapper} implementation that ensures that column values longer than a specified length will be truncated.
 *
 * @author Randall Hauch
 */
public class TruncateColumn implements ColumnMapper {

    private final TruncatingValueConverter converter;

    /**
     * Create a {@link ColumnMapper} that truncates column values to a maximum length.
     *
     * @param maxLength the maximum number of characters allowed in values
     * @throws IllegalArgumentException if the {@code maxLength} is not positive
     */
    public TruncateColumn(int maxLength) {
        if (maxLength < 0) {
            throw new IllegalArgumentException("Maximum length must be non-negative");
        }
        this.converter = new TruncatingValueConverter(maxLength);
    }

    @Override
    public ValueConverter create(Column column) {
        return isTruncationPossible(column) ? converter : ValueConverter.passthrough();
    }

    @Override
    public void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
        if (isTruncationPossible(column)) {
            schemaBuilder.parameter("truncateLength", Integer.toString(converter.maxLength));
        }
    }

    protected boolean isTruncationPossible(Column column) {
        // Possible when the length is unknown or greater than the max truncation length ...
        return column.length() < 0 || column.length() > converter.maxLength;
    }

    @Immutable
    protected static final class TruncatingValueConverter implements ValueConverter {
        protected final int maxLength;

        public TruncatingValueConverter(int maxLength) {
            this.maxLength = maxLength;
            assert this.maxLength >= 0;
        }

        @Override
        public Object convert(Object value) {
            if (value instanceof String) {
                String str = (String) value;
                if (str.length() > maxLength) {
                    return str.substring(0, maxLength);
                }
            }
            else if (value instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) value;
                if (buffer.limit() > maxLength) {
                    buffer.limit(maxLength);
                    return buffer.slice();
                }
            }
            return value;
        }
    }

}
