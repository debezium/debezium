/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractDatabaseHistoryTest {

    protected DatabaseHistory history;
    protected Map<String, Object> source1;
    protected Map<String, Object> source2;
    protected Tables tables;
    protected Tables t0;
    protected Tables t1;
    protected Tables t2;
    protected Tables t3;
    protected Tables t4;
    protected Tables all;
    protected DdlParser parser;

    @Before
    public void beforeEach() {
        parser = new MySqlAntlrDdlParser();
        tables = new Tables();
        t0 = new Tables();
        t1 = new Tables();
        t2 = new Tables();
        t3 = new Tables();
        t4 = new Tables();
        all = new Tables();
        source1 = server("abc");
        source2 = server("xyz");
        history = createHistory();
    }

    @After
    public void afterEach() {
        if (history != null) {
            history.stop();
        }
    }

    protected abstract DatabaseHistory createHistory();

    protected Map<String, Object> server(String serverName) {
        return Collect.linkMapOf("server", serverName);
    }

    protected Map<String, Object> position(String filename, long position, int entry) {
        return Collect.linkMapOf("file", filename, "position", position, "entry", entry);
    }

    protected void record(long pos, int entry, String ddl, Tables... update) {
        try {
            history.record(source1, position("a.log", pos, entry), "db", ddl);
        }
        catch (Throwable t) {
            fail(t.getMessage());
        }
        for (Tables tables : update) {
            if (tables != null) {
                parser.setCurrentSchema("db");
                parser.parse(ddl, tables);
            }
        }
    }

    protected Tables recover(long pos, int entry) {
        Tables result = new Tables();
        history.recover(source1, position("a.log", pos, entry), result, parser);
        return result;
    }

    @Test
    public void shouldRecordChangesAndRecoverToVariousPoints() {
        record(01, 0, "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", all, t3, t2, t1, t0);
        record(23, 1, "CREATE TABLE\nperson ( name VARCHAR(22) NOT NULL );", all, t3, t2, t1);
        record(30, 2, "CREATE TABLE address\n( street VARCHAR(22) NOT NULL );", all, t3, t2);
        record(32, 3, "ALTER TABLE address ADD city VARCHAR(22) NOT NULL;", all, t3);

        // Testing.Print.enable();
        if (Testing.Print.isEnabled()) {
            Testing.print("t0 = " + t0);
            Testing.print("t1 = " + t1);
            Testing.print("t2 = " + t2);
            Testing.print("t3 = " + t3);
        }

        assertThat(recover(01, 0)).isEqualTo(t0);
        assertThat(recover(01, 3)).isEqualTo(t0);
        assertThat(recover(10, 1)).isEqualTo(t0);
        assertThat(recover(22, 999999)).isEqualTo(t0);
        assertThat(recover(23, 0)).isEqualTo(t0);

        assertThat(recover(23, 1)).isEqualTo(t1);
        assertThat(recover(23, 2)).isEqualTo(t1);
        assertThat(recover(23, 3)).isEqualTo(t1);
        assertThat(recover(29, 999)).isEqualTo(t1);
        assertThat(recover(30, 1)).isEqualTo(t1);

        assertThat(recover(30, 2)).isEqualTo(t2);
        assertThat(recover(30, 3)).isEqualTo(t2);
        assertThat(recover(32, 2)).isEqualTo(t2);

        assertThat(recover(32, 3)).isEqualTo(t3);
        assertThat(recover(32, 4)).isEqualTo(t3);
        assertThat(recover(33, 0)).isEqualTo(t3);
        assertThat(recover(33, 0)).isEqualTo(all);
        assertThat(recover(1033, 4)).isEqualTo(t3);
        assertThat(recover(1033, 4)).isEqualTo(t3);
    }

}
