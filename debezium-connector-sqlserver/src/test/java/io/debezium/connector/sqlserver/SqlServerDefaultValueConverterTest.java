/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;

public class SqlServerDefaultValueConverterTest {

    private SqlServerDefaultValueConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new SqlServerDefaultValueConverter(() -> null, null);
    }

    @Test
    public void testParseUniqueIdentifierDefaultValue() {
        Column column = createColumn("uniqueidentifier");
        String defaultValue = "('6F9619FF-8B86-D011-B42D-00C04FC964FF')";

        Optional<Object> result = converter.parseDefaultValue(column, defaultValue);

        assertThat(result).isPresent();
        assertThat(result.get()).isInstanceOf(UUID.class);
        assertThat(result.get()).isEqualTo(UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF"));
    }

    @Test
    public void testParseUniqueIdentifierNullDefaultValue() {
        Column column = createColumn("uniqueidentifier");
        String defaultValue = "(NULL)";

        Optional<Object> result = converter.parseDefaultValue(column, defaultValue);

        assertThat(result).isEmpty();
    }

    private Column createColumn(String typeName) {
        return Column.editor()
                .name("test_column")
                .type(typeName)
                .create();
    }
}
