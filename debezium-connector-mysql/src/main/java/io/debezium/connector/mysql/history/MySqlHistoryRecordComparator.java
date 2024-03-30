/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.history;

import java.util.function.Predicate;

import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.connector.binlog.history.BinlogHistoryRecordComparator;

/**
 * @author Chris Cranford
 */
public class MySqlHistoryRecordComparator extends BinlogHistoryRecordComparator {
    public MySqlHistoryRecordComparator(Predicate<String> gtidSourceFilter, GtidSetFactory gtidSetFactory) {
        super(gtidSourceFilter, gtidSetFactory);
    }
}
