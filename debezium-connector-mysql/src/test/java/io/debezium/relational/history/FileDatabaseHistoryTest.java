/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.nio.file.Path;

import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class FileDatabaseHistoryTest extends AbstractDatabaseHistoryTest {

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("dbHistory.log");

    @Override
    @Before
    public void beforeEach() {
        Testing.Files.delete(TEST_FILE_PATH);
        super.beforeEach();
    }

    @Override
    protected DatabaseHistory createHistory() {
        DatabaseHistory history = new FileDatabaseHistory();
        history.configure(Configuration.create()
                .with(FileDatabaseHistory.FILE_PATH, TEST_FILE_PATH.toAbsolutePath().toString())
                .build(), null, DatabaseHistoryMetrics.NOOP, true);
        history.start();
        return history;
    }
}
