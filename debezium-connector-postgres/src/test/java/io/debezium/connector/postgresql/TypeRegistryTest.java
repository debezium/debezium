/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Array;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class TypeRegistryTest {

    @Test
    @FixFor("debezium/dbz#2525")
    void shouldTreatNullEnumValuesAsEmptyList() throws SQLException {
        assertThat(TypeRegistry.readEnumValues(null)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2525")
    void shouldReadEnumValuesFromSqlArray() throws SQLException {
        Array enumValuesArray = mock(Array.class);
        when(enumValuesArray.getArray()).thenReturn(new String[]{ "A", "B" });

        assertThat(TypeRegistry.readEnumValues(enumValuesArray)).containsExactly("A", "B");
    }
}
