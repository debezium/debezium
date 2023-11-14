/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import java.util.concurrent.ThreadFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.AbstractBinaryLogClientConfigurator;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;

/**
 * An {@link AbstractBinaryLogClientConfigurator} implementation for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbBinaryLogClientConfigurator extends AbstractBinaryLogClientConfigurator {

    public MariaDbBinaryLogClientConfigurator(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public BinaryLogClient configure(BinaryLogClient client, ThreadFactory threadFactory, AbstractConnectorConnection connection) {
        BinaryLogClient result = super.configure(client, threadFactory, connection);
        if (getConnectorConfig().includeSqlQuery()) {
            // Binlog client explicitly needs to be told to enable ANNOTATE_ROWS events, which is the
            // MariaDB equivalent of ROWS_QUERY. This must be done ahead of the connection to make
            // sure that the right negotiation bits are set during handshake.
            result.setUseSendAnnotateRowsEvent(true);
        }
        return result;
    }

    @Override
    public EventType getIncludeSqlQueryEventType() {
        return EventType.ANNOTATE_ROWS;
    }

    @Override
    protected void configureReplicaCompatibility(BinaryLogClient client) {
        // This makes sure BEGIN events are emitted via QUERY events rather than GTIDs.
        client.setMariaDbSlaveCapability(2);
    }

    @Override
    protected EventDeserializer createEventDeserializer() {
        EventDeserializer eventDeserializer = super.createEventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        return eventDeserializer;
    }

}
