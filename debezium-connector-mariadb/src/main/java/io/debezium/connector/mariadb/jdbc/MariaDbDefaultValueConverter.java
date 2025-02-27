/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;

/**
 * @author Chris Cranford
 */
public class MariaDbDefaultValueConverter extends BinlogDefaultValueConverter {
    public MariaDbDefaultValueConverter(MariaDbValueConverters valueConverter) {
        super(valueConverter);
    }
}
