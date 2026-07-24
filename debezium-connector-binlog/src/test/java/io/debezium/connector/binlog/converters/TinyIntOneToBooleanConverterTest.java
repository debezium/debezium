/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link TinyIntOneToBooleanConverter}, covering the value conversion
 * dispatch — in particular that any nonzero value converts to {@code true}, matching
 * MySQL semantics where {@code SELECT -1 IS TRUE} returns 1.
 *
 * @author minleejae
 */
class TinyIntOneToBooleanConverterTest {

    private static final String DATA_COLLECTION = "appdb.users";

    @Test
    void zeroConvertsToFalse() {
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert((short) 0)).isEqualTo(false);
    }

    @Test
    void positiveValuesConvertToTrue() {
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert((short) 1)).isEqualTo(true);
        assertThat(reg.converter.convert((short) 2)).isEqualTo(true);
        assertThat(reg.converter.convert((short) 127)).isEqualTo(true);
    }

    @Test
    void negativeValuesConvertToTrue() {
        // MySQL considers any nonzero value true: SELECT -1 IS TRUE returns 1, and the
        // default BOOLEAN conversion (JdbcValueConverters#convertBoolean) agrees.
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert((short) -1)).isEqualTo(true);
        assertThat(reg.converter.convert((short) -128)).isEqualTo(true);
    }

    @Test
    void booleanInputPassesThrough() {
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert(Boolean.TRUE)).isEqualTo(true);
        assertThat(reg.converter.convert(Boolean.FALSE)).isEqualTo(false);
    }

    @Test
    void numericStringsConvertToBoolean() {
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert("0")).isEqualTo(false);
        assertThat(reg.converter.convert("1")).isEqualTo(true);
        assertThat(reg.converter.convert("-1")).isEqualTo(true);
    }

    @Test
    void nonNumericStringsFallBackToBooleanParsing() {
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert("true")).isEqualTo(true);
        assertThat(reg.converter.convert("false")).isEqualTo(false);
    }

    @Test
    void nullConvertsToNullForOptionalColumn() {
        final Registration reg = registerOn(newConfigured(), tinyIntOneColumn("b"));
        assertThat(reg.converter.convert(null)).isNull();
    }

    private static TinyIntOneToBooleanConverter newConfigured() {
        final TinyIntOneToBooleanConverter converter = new TinyIntOneToBooleanConverter();
        converter.configure(new Properties());
        return converter;
    }

    private static RelationalColumn tinyIntOneColumn(String name) {
        final RelationalColumn col = Mockito.mock(RelationalColumn.class);
        Mockito.when(col.name()).thenReturn(name);
        Mockito.when(col.dataCollection()).thenReturn(DATA_COLLECTION);
        Mockito.when(col.typeName()).thenReturn("TINYINT");
        Mockito.when(col.length()).thenReturn(OptionalInt.of(1));
        Mockito.when(col.isOptional()).thenReturn(true);
        return col;
    }

    private static Registration registerOn(TinyIntOneToBooleanConverter converter, RelationalColumn column) {
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
