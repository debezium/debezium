/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;

/**
 * Unit tests for {@link ColumnMetaData}, in particular the pgvector dimension handling (dbz#2220).
 */
public class ColumnMetaDataTest {

    private static PostgresType vectorType() {
        // No parent/element type, so the builder never touches the (null) TypeRegistry; the vector
        // branch resolves length from the modifier without touching TypeInfo, so null is fine here.
        return new PostgresType.Builder(null, "vector", 99999, Types.OTHER, TypeRegistry.NO_TYPE_MODIFIER, null).build();
    }

    @Test
    @FixFor("debezium/dbz#2220")
    public void shouldReadPgVectorDimensionFromModifier() {
        final ColumnMetaData column = new ColumnMetaData("v", vectorType(), false, true, false, null, 3);
        assertThat(column.getLength()).isEqualTo(3);
        assertThat(column.getScale()).isEqualTo(0);
        // Single dimension modifier: vector(3), not vector(3,0).
        assertThat(column.getTypeName()).isEqualTo("vector(3)");
    }

    @Test
    @FixFor("debezium/dbz#2220")
    public void shouldLeavePgVectorWithoutDimensionUnset() {
        final ColumnMetaData column = new ColumnMetaData("v", vectorType(), false, true, false, null, TypeRegistry.NO_TYPE_MODIFIER);
        assertThat(column.getLength()).isEqualTo(Column.UNSET_INT_VALUE);
        assertThat(column.getTypeName()).isEqualTo("vector");
    }
}
