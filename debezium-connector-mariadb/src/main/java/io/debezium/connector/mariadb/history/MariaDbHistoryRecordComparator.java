/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.history;

import java.util.function.Predicate;

import io.debezium.connector.binlog.gtid.GtidSetFactory;
import io.debezium.connector.binlog.history.BinlogHistoryRecordComparator;

/**
 * Schema history record comparator implementation for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbHistoryRecordComparator extends BinlogHistoryRecordComparator {
    public MariaDbHistoryRecordComparator(Predicate<String> gtidSourceFilter, GtidSetFactory gtidSetFactory) {
        super(gtidSourceFilter, gtidSetFactory);
    }
}
