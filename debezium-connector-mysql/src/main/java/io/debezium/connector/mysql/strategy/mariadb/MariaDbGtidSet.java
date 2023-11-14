/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import java.util.function.Predicate;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.GtidSet;

/**
 * @author Chris Cranford
 */
public class MariaDbGtidSet implements GtidSet {

    public MariaDbGtidSet(String gtid) {
    }

    @Override
    public boolean isEmpty() {
        throw new DebeziumException("NYI");
    }

    @Override
    public GtidSet retainAll(Predicate<String> sourceFilter) {
        throw new DebeziumException("NYI");
    }

    @Override
    public boolean isContainedWithin(GtidSet other) {
        throw new DebeziumException("NYI");
    }

    @Override
    public GtidSet with(GtidSet other) {
        throw new DebeziumException("NYI");
    }

    @Override
    public GtidSet getGtidSetBeginning() {
        throw new DebeziumException("NYI");
    }

    @Override
    public boolean contains(String gtid) {
        throw new DebeziumException("NYI");
    }

    @Override
    public GtidSet subtract(GtidSet other) {
        throw new DebeziumException("NYI");
    }
}
