/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Map;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogSourceInfoTest;
import io.debezium.connector.binlog.history.BinlogHistoryRecordComparator;
import io.debezium.connector.mysql.gtid.MySqlGtidSetFactory;
import io.debezium.connector.mysql.history.MySqlHistoryRecordComparator;

public class SourceInfoTest extends BinlogSourceInfoTest<SourceInfo, MySqlOffsetContext> {
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
        return new MySqlHistoryRecordComparator(gtidFilter, new MySqlGtidSetFactory());
    }

    @Override
    protected MySqlOffsetContext createInitialOffsetContext(Configuration configuration) {
        return MySqlOffsetContext.initial(new MySqlConnectorConfig(configuration));
    }

    @Override
    protected MySqlOffsetContext loadOffsetContext(Configuration configuration, Map<String, ?> offsets) {
        return new MySqlOffsetContext.Loader(new MySqlConnectorConfig(configuration)).load(offsets);
    }
}
