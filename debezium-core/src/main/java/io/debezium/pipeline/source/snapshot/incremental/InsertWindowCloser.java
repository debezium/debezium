/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public class InsertWindowCloser implements WatermarkWindowCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertWindowCloser.class);
    public static final String INSERT_STATEMENT = "INSERT INTO %s VALUES (?, ?, null)";

    private final JdbcConnection jdbcConnection;
    private final String signalWindowStatement;

    public InsertWindowCloser(JdbcConnection jdbcConnection, String signalTable) {
        this.jdbcConnection = jdbcConnection;
        signalWindowStatement = String.format(INSERT_STATEMENT, signalTable);
    }

    @Override
    public void closeWindows(Partition partition, OffsetContext offsetContext, String chunkId) throws SQLException {

        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            LOGGER.trace("Emitting close window for chunk = '{}'", chunkId);
            x.setString(1, chunkId + "-close");
            x.setString(2, CloseIncrementalSnapshotWindow.NAME);
        });

        jdbcConnection.commit();
    }
}
