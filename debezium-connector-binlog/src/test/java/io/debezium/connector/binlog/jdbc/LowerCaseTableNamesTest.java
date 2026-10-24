/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

class LowerCaseTableNamesTest {

    @Test
    void shouldPreserveCaseSensitiveTableIdWithoutResolvingDatabase() {
        final var tableId = new TableId("MyDb", "SomeSchema", "Member");

        assertThat(LowerCaseTableNames.CASE_SENSITIVE.toServerTableId(tableId, name -> {
            throw new AssertionError("Case-sensitive names should not require resolution");
        })).isSameAs(tableId);
    }

    @Test
    void shouldLowercaseStoredTableNameAndPreserveSchema() {
        final var tableId = new TableId("MyDb", "SomeSchema", "Member");

        assertThat(LowerCaseTableNames.STORED_IN_LOWERCASE.toServerTableId(tableId, name -> name.toLowerCase(Locale.ROOT)))
                .isEqualTo(new TableId("mydb", "SomeSchema", "member"));
    }

    @Test
    void shouldPreserveTableCaseAndResolveDeclaredDatabaseName() {
        final var tableId = new TableId("mydb", "SomeSchema", "Member");
        final Map<String, String> databases = Map.of("mydb", "MyDb");

        assertThat(LowerCaseTableNames.CASE_INSENSITIVE.toServerTableId(tableId, databases::get))
                .isEqualTo(new TableId("MyDb", "SomeSchema", "Member"));
    }

    @Test
    void shouldHandleTableIdsWithoutCatalogs() {
        final var tableId = new TableId(null, "SomeSchema", "Member");

        assertThat(LowerCaseTableNames.STORED_IN_LOWERCASE.toServerTableId(tableId, name -> null))
                .isEqualTo(new TableId(null, "SomeSchema", "member"));
    }

    @Test
    void shouldPreserveNullTableIdsWithoutResolvingDatabase() {
        for (LowerCaseTableNames setting : LowerCaseTableNames.values()) {
            assertThat(setting.toServerTableId(null, name -> {
                throw new AssertionError("Null table ids should not require resolution");
            })).isNull();
        }
    }
}
