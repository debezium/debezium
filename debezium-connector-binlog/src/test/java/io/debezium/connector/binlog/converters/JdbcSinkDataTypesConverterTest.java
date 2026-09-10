/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.doc.FixFor;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link JdbcSinkDataTypesConverter}, covering the boolean selector's
 * default-value fallback for non-optional columns — in particular that a parsed column
 * default reaching the converter as a {@link Number} is handled. With {@code BOOLEAN}
 * aliases normalized to {@code TINYINT(1)}, the default of a {@code BOOLEAN NOT NULL
 * DEFAULT TRUE} column is parsed through the numeric path and arrives as an
 * {@code Integer}, not a {@code Boolean}.
 *
 * @author minleejae
 */
class JdbcSinkDataTypesConverterTest {

    private static final String DATA_COLLECTION = "appdb.orders";

    @Test
    @FixFor("debezium/dbz#2189")
    void numberDefaultIsAppliedForNullValueOnNonOptionalColumn() {
        // BOOLEAN NOT NULL DEFAULT TRUE normalizes to TINYINT(1), whose parsed default is Integer 1
        final Registration reg = registerBoolean(booleanColumn("b1", false, 1));
        assertThat(reg.converter.convert(null)).isEqualTo((short) 1);
    }

    @Test
    @FixFor("debezium/dbz#2189")
    void booleanDefaultIsAppliedForNullValueOnNonOptionalColumn() {
        // Pre-normalization history topics can still replay BOOLEAN columns whose default is a Boolean
        final Registration reg = registerBoolean(booleanColumn("b1", false, Boolean.TRUE));
        assertThat(reg.converter.convert(null)).isEqualTo((short) 1);
    }

    @Test
    void nullConvertsToNullForOptionalColumn() {
        final Registration reg = registerBoolean(booleanColumn("b1", true, null));
        assertThat(reg.converter.convert(null)).isNull();
    }

    @Test
    void valuesConvertIndependentlyOfDefault() {
        final Registration reg = registerBoolean(booleanColumn("b1", false, 1));
        assertThat(reg.converter.convert(Boolean.TRUE)).isEqualTo((short) 1);
        assertThat(reg.converter.convert((byte) 0)).isEqualTo((short) 0);
        assertThat(reg.converter.convert("1")).isEqualTo((short) 1);
    }

    private static RelationalColumn booleanColumn(String name, boolean optional, Object defaultValue) {
        final RelationalColumn col = Mockito.mock(RelationalColumn.class);
        Mockito.when(col.name()).thenReturn(name);
        Mockito.when(col.dataCollection()).thenReturn(DATA_COLLECTION);
        Mockito.when(col.isOptional()).thenReturn(optional);
        Mockito.when(col.hasDefaultValue()).thenReturn(defaultValue != null);
        Mockito.when(col.defaultValue()).thenReturn(defaultValue);
        return col;
    }

    private static Registration registerBoolean(RelationalColumn column) {
        final JdbcSinkDataTypesConverter converter = new JdbcSinkDataTypesConverter();
        final Properties props = new Properties();
        props.setProperty(JdbcSinkDataTypesConverter.SELECTOR_BOOLEAN_PROPERTY, ".*");
        converter.configure(props);

        final AtomicReference<SchemaBuilder> schemaRef = new AtomicReference<>();
        final AtomicReference<CustomConverter.Converter> convRef = new AtomicReference<>();
        converter.converterFor(column, (schema, conv) -> {
            schemaRef.set(schema);
            convRef.set(conv);
        });
        assertThat(schemaRef.get()).as("converter did not register a schema for column %s", column.name()).isNotNull();
        return new Registration(schemaRef.get().build(), convRef.get());
    }

    private record Registration(Schema schema, CustomConverter.Converter converter) {
    }
}
