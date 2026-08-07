/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

class PostgresValueConverterTest {

    private static final int NUMERIC_ARRAY_OID = 2003;

    private static final Column NUMERIC_ARRAY_COLUMN = Column.editor()
            .name("numeric_array")
            .type("_numeric")
            .jdbcType(Types.ARRAY)
            .nativeType(NUMERIC_ARRAY_OID)
            .length(9)
            .scale(3)
            .optional(false)
            .create();

    private static final Field NUMERIC_ARRAY_FIELD = new Field("numeric_array", 0,
            SchemaBuilder.array(Decimal.builder(3).optional().build()).build());

    private static final List<BigDecimal> VALUES = List.of(new BigDecimal("1.100"), new BigDecimal("2.200"));

    private final PostgresValueConverter converter = PostgresValueConverter.of(
            new PostgresConnectorConfig(Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                    .build()),
            StandardCharsets.UTF_8,
            null);

    private final ValueConverter elementConverter = value -> value;

    @Test
    public void shouldConvertArrayFromJdbcArrayThatIsNotAPgArray() {
        Object converted = converter.convertArray(NUMERIC_ARRAY_COLUMN, NUMERIC_ARRAY_FIELD, PostgresType.UNKNOWN,
                elementConverter, jdbcArrayOf(VALUES.toArray()));

        assertThat(converted).isEqualTo(VALUES);
    }

    @Test
    public void shouldConvertArrayFromList() {
        Object converted = converter.convertArray(NUMERIC_ARRAY_COLUMN, NUMERIC_ARRAY_FIELD, PostgresType.UNKNOWN,
                elementConverter, VALUES);

        assertThat(converted).isEqualTo(VALUES);
    }

    @Test
    public void shouldRejectValueThatIsNeitherAnArrayNorAList() {
        assertThatThrownBy(() -> converter.convertArray(NUMERIC_ARRAY_COLUMN, NUMERIC_ARRAY_FIELD,
                PostgresType.UNKNOWN, elementConverter, "{1.100,2.200}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected value for JDBC type " + Types.ARRAY);
    }

    private static Array jdbcArrayOf(Object[] values) {
        return (Array) Proxy.newProxyInstance(
                PostgresValueConverterTest.class.getClassLoader(),
                new Class<?>[]{ Array.class },
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getArray":
                            return values;
                        case "getBaseType":
                            return Types.NUMERIC;
                        case "toString":
                            return "java.sql.Array" + List.of(values);
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }
}
