/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultRelationalMetadataFactory;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedRelationalMetadataFactory;
import io.debezium.relational.RelationalMetadataFactory;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;

/**
 * Verifies that {@link AbstractDdlParser} creates columns through its {@link RelationalMetadataFactory}.
 *
 * @author Chris Cranford
 */
public class AbstractDdlParserTest {

    private static class TestDdlParser extends AbstractDdlParser {

        TestDdlParser() {
            super(true, true);
        }

        @Override
        protected SystemVariables createNewSystemVariablesInstance() {
            return new SystemVariables();
        }

        @Override
        public DdlChanges parse(String ddlContent, Tables databaseTables) {
            return ddlChanges;
        }

        Column constant(String columnName, String constantValue) {
            return createColumnFromConstant(columnName, constantValue);
        }
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldDefaultToStandardFactory() {
        final TestDdlParser parser = new TestDdlParser();
        assertThat(parser.metadataFactory()).isSameAs(DefaultRelationalMetadataFactory.INSTANCE);
        assertThat(parser.constant("c", "'abc'")).isNotInstanceOf(ExtendedColumn.class);
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldCreateConstantColumnsThroughMetadataFactory() {
        final TestDdlParser parser = new TestDdlParser() {
            @Override
            protected RelationalMetadataFactory metadataFactory() {
                return ExtendedRelationalMetadataFactory.INSTANCE;
            }
        };

        final Column column = parser.constant("c", "'abc'");
        assertThat(column).isInstanceOf(ExtendedColumn.class);
        assertThat(column.name()).isEqualTo("c");
        assertThat(column.typeName()).isEqualTo("CHAR");
        assertThat(column.jdbcType()).isEqualTo(Types.CHAR);
        assertThat(column.length()).isEqualTo(3);
    }
}
