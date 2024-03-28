/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.gtid;

import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.gtid.GtidSetFactory;

/**
 * MariaDB-specific implementation of the {@link GtidSetFactory} for creating {@link GtidSet}s.
 *
 * @author Chris Cranford
 */
public class MariaDbGtidSetFactory implements GtidSetFactory {
    @Override
    public GtidSet createGtidSet(String gtid) {
        return new MariaDbGtidSet(gtid);
    }
}
