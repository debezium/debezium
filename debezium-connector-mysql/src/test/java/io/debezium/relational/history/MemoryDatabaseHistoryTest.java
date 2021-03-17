/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * @author Randall Hauch
 */
public class MemoryDatabaseHistoryTest extends AbstractDatabaseHistoryTest {

    @Override
    protected DatabaseHistory createHistory() {
        return new MemoryDatabaseHistory();
    }
}
