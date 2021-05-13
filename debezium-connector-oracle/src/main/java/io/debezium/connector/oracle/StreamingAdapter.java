/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * Contract that defines unique behavior for each possible {@code connection.adapter}.
 *
 * @author Chris Cranford
 */
public interface StreamingAdapter {

    void configure(OracleConnectorConfig connectorConfig, OracleConnection connection);

    String getType();

    HistoryRecordComparator getHistoryRecordComparator();

    OffsetContext.Loader getOffsetContextLoader();

    StreamingChangeEventSource getSource(OffsetContext offsetContext, EventDispatcher<TableId> dispatcher,
                                         ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                         OracleTaskContext taskContext, Configuration jdbcConfig,
                                         OracleStreamingChangeEventSourceMetrics streamingMetrics);

    boolean getTablenameCaseInsensitivity();
}
