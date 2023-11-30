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
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class DeleteWindowCloser<P extends Partition, T extends DataCollectionId> implements WatermarkWindowCloser {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteWindowCloser.class);
    public static final String DELETE_STATEMENT = "DELETE FROM %s WHERE id = ?";

    private final JdbcConnection jdbcConnection;
    private final String signalWindowStatement;
    private final SignalBasedIncrementalSnapshotChangeEventSource<P, T> incrementalSnapshotChangeEventSource;

    public DeleteWindowCloser(JdbcConnection jdbcConnection, String signalTable,
                              SignalBasedIncrementalSnapshotChangeEventSource<P, T> incrementalSnapshotChangeEventSource) {
        this.jdbcConnection = jdbcConnection;
        signalWindowStatement = String.format(DELETE_STATEMENT, signalTable);
        this.incrementalSnapshotChangeEventSource = incrementalSnapshotChangeEventSource;
    }

    @Override
    public void closeWindows(Partition partition, OffsetContext offsetContext, String chunkId) throws SQLException {

        jdbcConnection.prepareUpdate(signalWindowStatement, x -> {
            LOGGER.trace("Deleting open window for chunk = '{}'", chunkId);
            x.setString(1, chunkId + "-open");
        });

        // Since the close event is not written into signal data collection we need to explicit close the window.
        try {
            incrementalSnapshotChangeEventSource.closeWindow((P) partition, chunkId + "-close", offsetContext);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Failed to close window {} successful.", chunkId);
            Thread.currentThread().interrupt();
        }

        jdbcConnection.commit();
    }
}
