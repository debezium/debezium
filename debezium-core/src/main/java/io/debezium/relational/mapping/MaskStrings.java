/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.sql.Types;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * A {@link ColumnMapper} implementation that ensures that string values are masked by a predefined value.
 * 
 * @author Randall Hauch
 */
public class MaskStrings implements ColumnMapper {

    private final MaskingValueConverter converter;

    /**
     * Create a {@link ColumnMapper} that masks string values with a predefined value.
     * 
     * @param maskValue the value that should be used in place of the actual value; may not be null
     * @throws IllegalArgumentException if the {@code maxLength} is not positive
     */
    public MaskStrings(String maskValue) {
        if (maskValue == null) throw new IllegalArgumentException("Mask value may not be null");
        this.converter = new MaskingValueConverter(maskValue);
    }

    @Override
    public ValueConverter create(Column column) {
        switch (column.jdbcType()) {
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
                return converter;
            default:
                return ValueConverter.passthrough();
        }
    }

    @Override
    public void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
        schemaBuilder.parameter("masked", "true");
    }

    @Immutable
    protected static final class MaskingValueConverter implements ValueConverter {
        protected final String maskValue;

        public MaskingValueConverter(String maskValue) {
            this.maskValue = maskValue;
            assert this.maskValue != null;
        }

        @Override
        public Object convert(Object value) {
            return maskValue;
        }
    }
}
