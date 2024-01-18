/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mysql;

import java.util.function.Predicate;

import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.strategy.AbstractHistoryRecordComparator;

/**
 * @author Chris Cranford
 */
public class MySqlHistoryRecordComparator extends AbstractHistoryRecordComparator {

    public MySqlHistoryRecordComparator(Predicate<String> gtidSourceFilter) {
        super(gtidSourceFilter);
    }

    @Override
    protected GtidSet createGtidSet(String gtidSet) {
        return new MySqlGtidSet(gtidSet);
    }

}
