/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb.hybrid;

import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.BinaryLogClientConfigurator;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbBinaryLogClientConfigurator;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnectorAdapter;

/**
 * This connector adapter provides a hybrid configuration where the user connects to a
 * MariaDB target system; however, uses the MySQL driver.
 *
 * @author Chris Cranford
 */
public class MariaDbHybridConnectorAdapter extends MySqlConnectorAdapter {

    // todo: Do we want to consider supporting this mode at all?

    private final MariaDbBinaryLogClientConfigurator binaryLogClientConfigurator;

    public MariaDbHybridConnectorAdapter(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.binaryLogClientConfigurator = new MariaDbBinaryLogClientConfigurator(connectorConfig);
    }

    @Override
    public BinaryLogClientConfigurator getBinaryLogClientConfigurator() {
        return binaryLogClientConfigurator;
    }

    @Override
    public String getRecordingQueryFromEvent(EventData eventData) {
        return ((AnnotateRowsEventData) eventData).getRowsQuery();
    }

}
