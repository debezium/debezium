/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.gtid;

import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.gtid.GtidSetFactory;

/**
 * MySQL-specific implementation of the {@link GtidSetFactory} for creating {@link GtidSet}s.
 *
 * @author Chris Cranford
 */
public class MySqlGtidSetFactory implements GtidSetFactory {
    @Override
    public GtidSet createGtidSet(String gtid) {
        return new MySqlGtidSet(gtid);
    }
}
