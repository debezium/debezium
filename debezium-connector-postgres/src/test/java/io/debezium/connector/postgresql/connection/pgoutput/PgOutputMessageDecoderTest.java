/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.doc.FixFor;
import io.debezium.relational.Table;

public class PgOutputMessageDecoderTest {

    @Test
    @FixFor("debezium/dbz#2534")
    void shouldPreserveSchemaCommentsWhenResolvingRelationMetadata() {
        final PostgresType postgresType = new PostgresType.Builder(
                null, "int4", 23, Types.INTEGER, TypeRegistry.NO_TYPE_MODIFIER, null).build();
        final ColumnMetaData column = new ColumnMetaData(
                "id", postgresType, true, false, false, null, "Column comment", TypeRegistry.NO_TYPE_MODIFIER, "int4");
        final PgOutputRelationMetaData metadata = new PgOutputRelationMetaData(
                1, "public", "comment_test", "Table comment", List.of(column), List.of("id"));

        final Table table = PgOutputMessageDecoder.resolveRelationFromMetadata(metadata);

        assertThat(table.comment()).isEqualTo("Table comment");
        assertThat(table.columnWithName("id").comment()).isEqualTo("Column comment");
    }
}