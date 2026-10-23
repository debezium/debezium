/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.core.Oid;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;

/**
 * Parsing of {@code text[]} and {@code varchar[]} column defaults (debezium/dbz#1269). Only plain array literals are
 * propagated; any other form must yield no default rather than a wrong one.
 */
class PostgresTextArrayDefaultValueTest {

    private PostgresDefaultValueConverter defaultValueConverter;

    @BeforeEach
    void setUp() {
        final TypeRegistry typeRegistry = mock(TypeRegistry.class);
        when(typeRegistry.getRegisteredTypes()).thenReturn(Collections.emptyMap());
        stubStringArrayType(typeRegistry, Oid.TEXT_ARRAY, Oid.TEXT, "text");
        stubStringArrayType(typeRegistry, Oid.VARCHAR_ARRAY, Oid.VARCHAR, "varchar");

        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                .build());
        final PostgresValueConverter valueConverter = PostgresValueConverter.of(connectorConfig, StandardCharsets.UTF_8, typeRegistry);
        defaultValueConverter = new PostgresDefaultValueConverter(valueConverter, null, typeRegistry);
    }

    @Test
    @FixFor("debezium/dbz#1269")
    void shouldParseEmptyArrayLiteralDefault() {
        assertThat(parse("_text", Oid.TEXT_ARRAY, "'{}'::text[]")).contains(Collections.emptyList());
        assertThat(parse("_varchar", Oid.VARCHAR_ARRAY, "'{}'::character varying[]")).contains(Collections.emptyList());
    }

    @Test
    @FixFor("debezium/dbz#1269")
    void shouldParsePlainArrayLiteralDefault() {
        assertThat(parse("_text", Oid.TEXT_ARRAY, "'{a,b}'::text[]")).contains(List.of("a", "b"));
        assertThat(parse("_varchar", Oid.VARCHAR_ARRAY, "'{a,b}'::character varying[]")).contains(List.of("a", "b"));
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#1269")
    @ValueSource(strings = {
            "ARRAY['a'::text, 'b'::text]",
            "'{\"a,b\",c}'::text[]",
            "'{\"a\\\"b\",\"\"}'::text[]",
            "'{{a},{b}}'::text[]",
            "'{NULL,a}'::text[]",
            "NULL::text[]"
    })
    void shouldNotPropagateUnsupportedArrayDefault(String defaultExpression) {
        assertThat(parse("_text", Oid.TEXT_ARRAY, defaultExpression)).isEmpty();
        assertThat(parse("_varchar", Oid.VARCHAR_ARRAY, defaultExpression.replace("::text", "::character varying"))).isEmpty();
    }

    private Optional<Object> parse(String typeName, int arrayOid, String defaultExpression) {
        final Column column = Column.editor()
                .name("col")
                .type(typeName)
                .jdbcType(Types.ARRAY)
                .nativeType(arrayOid)
                .optional(false)
                .defaultValueExpression(defaultExpression)
                .create();
        return defaultValueConverter.parseDefaultValue(column, defaultExpression);
    }

    private static void stubStringArrayType(TypeRegistry typeRegistry, int arrayOid, int elementOid, String elementName) {
        final PostgresType elementType = mock(PostgresType.class);
        when(elementType.getName()).thenReturn(elementName);
        when(elementType.getJdbcId()).thenReturn(Types.VARCHAR);
        when(elementType.getOid()).thenReturn(elementOid);
        when(elementType.isEnumType()).thenReturn(false);
        when(elementType.isArrayType()).thenReturn(false);

        final PostgresType arrayType = mock(PostgresType.class);
        when(arrayType.getElementType()).thenReturn(elementType);

        when(typeRegistry.get(arrayOid)).thenReturn(arrayType);
        when(typeRegistry.get(elementOid)).thenReturn(elementType);
    }
}
