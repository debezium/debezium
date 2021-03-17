/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParserListener.EventType;

public class DdlChangesTest {

    private DdlChanges changes;
    private DdlParser parser;
    private Tables tables;

    @Before
    public void beforeEach() {
        parser = new MySqlAntlrDdlParser();
        changes = parser.getDdlChanges();
        tables = new Tables();
    }

    @Test
    public void shouldParseMultipleStatementsWithDefaultDatabase() {
        parser.setCurrentSchema("mydb");
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator()
                + "-- This is a comment" + System.lineSeparator()
                + "DROP TABLE foo;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // table created and dropped

        changes.groupEventsByDatabase((dbName, list) -> {
            assertThat(dbName).isEqualTo("mydb");
            assertThat(list.size()).isEqualTo(2);
            assertThat(list.get(0).type()).isEqualTo(EventType.CREATE_TABLE);
            assertThat(list.get(1).type()).isEqualTo(EventType.DROP_TABLE);
        });
    }

    @Test
    public void shouldParseMultipleStatementsWithFullyQualifiedDatabase() {
        parser.setCurrentSchema("mydb");
        String ddl = "CREATE TABLE other.foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator()
                + "-- This is a comment" + System.lineSeparator()
                + "DROP TABLE other.foo;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // table created and dropped

        changes.groupEventsByDatabase((dbName, list) -> {
            assertThat(dbName).isEqualTo("other");
            assertThat(list.size()).isEqualTo(2);
            assertThat(list.get(0).type()).isEqualTo(EventType.CREATE_TABLE);
            assertThat(list.get(1).type()).isEqualTo(EventType.DROP_TABLE);
        });
    }

    @Test
    public void shouldParseMultipleStatementsWithNoCurrentSchemaAndFullyQualifiedDatabase() {
        String ddl = "CREATE TABLE other.foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator()
                + "-- This is a comment" + System.lineSeparator()
                + "DROP TABLE other.foo;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // table created and dropped

        for (int i = 0; i != 5; ++i) {
            changes.groupEventsByDatabase((dbName, list) -> {
                assertThat(dbName).isEqualTo("other");
                assertThat(list.size()).isEqualTo(2);
                assertThat(list.get(0).type()).isEqualTo(EventType.CREATE_TABLE);
                assertThat(list.get(1).type()).isEqualTo(EventType.DROP_TABLE);
            });
        }

        changes.reset();
        changes.groupEventsByDatabase((dbName, list) -> {
            fail("Should not have any changes");
        });
    }

}
