/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.Map;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogSourceInfoTest;
import io.debezium.connector.binlog.history.BinlogHistoryRecordComparator;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSetFactory;
import io.debezium.connector.mariadb.history.MariaDbHistoryRecordComparator;

/**
 * @author Chris Cranford
 */
public class SourceInfoTest extends BinlogSourceInfoTest<SourceInfo, MariaDbOffsetContext> {
    @Override
    protected String getModuleName() {
        return Module.name();
    }

    @Override
    protected String getModuleVersion() {
        return Module.version();
    }

    @Override
    protected BinlogHistoryRecordComparator getHistoryRecordComparator(Predicate<String> gtidFilter) {
        return new MariaDbHistoryRecordComparator(gtidFilter, new MariaDbGtidSetFactory());
    }

    @Override
    protected MariaDbOffsetContext createInitialOffsetContext(Configuration configuration) {
        return MariaDbOffsetContext.initial(new MariaDbConnectorConfig(configuration));
    }

    @Override
    protected MariaDbOffsetContext loadOffsetContext(Configuration configuration, Map<String, ?> offsets) {
        return new MariaDbOffsetContext.Loader(new MariaDbConnectorConfig(configuration)).load(offsets);
    }
}
