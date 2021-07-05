/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.storage;

import io.debezium.config.Configuration;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryMetrics;
import io.debezium.relational.history.FileDatabaseHistory;
import org.junit.Before;
import org.junit.Test;

import static io.debezium.server.storage.JdbcDatabaseHistory.*;

/**
 * @author Randall Hauch
 */
public class JdbcDatabaseHistoryTest extends AbstractDatabaseHistoryTest {

    String dbFile = "/tmp/test.db";

    @Override
    @Before
    public void beforeEach() {
        super.beforeEach();
    }

    @Override
    protected DatabaseHistory createHistory() {
        DatabaseHistory history = new JdbcDatabaseHistory();
        history.configure(Configuration.create()
                .with(JDBC_URI, "jdbc:sqlite:"+dbFile)
                .with(JDBC_USER, "user")
                .with(JDBC_PASSWORD, "pass")
                .build(), null, DatabaseHistoryMetrics.NOOP, true);
        history.start();
        return history;
    }

    @Override
    @Test
    public void shouldRecordChangesAndRecoverToVariousPoints() {
        super.shouldRecordChangesAndRecoverToVariousPoints();
    }
}
