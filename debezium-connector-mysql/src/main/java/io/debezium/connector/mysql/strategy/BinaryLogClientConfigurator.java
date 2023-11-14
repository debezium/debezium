/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy;

import java.util.concurrent.ThreadFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;

/**
 * @author Chris Cranford
 */
public interface BinaryLogClientConfigurator {
    /**
     * Configures the provided Binary Log Client instance.
     *
     * @param client the client instance ot be configured; should not be null
     * @param threadFactory the thread factory to be used; should not be null
     * @param connection the connector's JDBC connection; should not be null
     *
     * @return the configured binary log client instance
     */
    BinaryLogClient configure(BinaryLogClient client, ThreadFactory threadFactory, AbstractConnectorConnection connection);

    EventType getIncludeSqlQueryEventType();
}
