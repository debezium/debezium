/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamingChangeEventSource.class);

    private final PostgresConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresOffsetContext offsetContext;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresTaskContext taskContext;
    private final ReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference<>();
    private Long lastCompletelyProcessedLsn;
    private final Snapshotter snapshotter;
    private final Metronome pauseNoMessage;

    public PostgresStreamingChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter, PostgresOffsetContext offsetContext, PostgresConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, PostgresSchema schema, PostgresTaskContext taskContext, ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = (offsetContext != null) ? offsetContext :
            PostgresOffsetContext.initialContext(connectorConfig, connection, clock);
        pauseNoMessage = Metronome.sleeper(taskContext.getConfig().getPollInterval(), Clock.SYSTEM);
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Streaming is not enabled in currect configuration");
            return;
        }

        try {
            if (offsetContext.hasLastKnownPosition()) {
                // start streaming from the last recorded position in the offset
                final Long lsn = offsetContext.lsn();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("retrieved latest position from stored offset '{}'", ReplicationConnection.format(lsn));
                }
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn));
            } else {
                LOGGER.info("no previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                replicationStream.compareAndSet(null, replicationConnection.startStreaming());
            }

            // refresh the schema so we have a latest view of the DB tables
            taskContext.refreshSchema(connection, true);

            this.lastCompletelyProcessedLsn = offsetContext.lsn();

            final ReplicationStream stream = this.replicationStream.get();
            while (context.isRunning()) {
                if (!stream.readPending(message -> {
                    final Long lsn = stream.lastReceivedLsn();
                    if (message == null) {
                        LOGGER.trace("Received empty message");
                        lastCompletelyProcessedLsn = lsn;
                        dispatcher.dispatchHeartbeatEvent(offsetContext);
                        return;
                    }
                    if (message.isLastEventForLsn()) {
                        lastCompletelyProcessedLsn = lsn;
                    }

                    final TableId tableId = PostgresSchema.parse(message.getTable());
                    Objects.requireNonNull(tableId);

                    offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), message.getTransactionId(), tableId, taskContext.getSlotXmin(connection));
                    dispatcher
                        .dispatchDataChangeEvent(
                                tableId,
                                new PostgresChangeRecordEmitter(
                                        offsetContext,
                                        clock,
                                        connectorConfig,
                                        schema,
                                        connection,
                                        message
                                )
                        );
                })) {
                    if (offsetContext.hasCompletelyProcessedPosition()) {
                        dispatcher.dispatchHeartbeatEvent(offsetContext);
                    }
                    pauseNoMessage.pause();
                }
            }
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            if (replicationConnection != null) {
                LOGGER.debug("stopping streaming...");
                //TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
                //replicationStream.close();
                // close the connection - this should also disconnect the current stream even if it's blocking
                try {
                    replicationConnection.close();
                }
                catch (Exception e) {
                }
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        try {
            ReplicationStream replicationStream = this.replicationStream.get();
            final Long lsn = (Long) offset.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);

            if (replicationStream != null && lsn != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Flushing LSN to server: {}", LogSequenceNumber.valueOf(lsn));
                }
                // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
                replicationStream.flushLsn(lsn);
            }
            else {
                LOGGER.debug("Streaming has already stopped, ignoring commit callback...");
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        PgConnection get() throws SQLException;
    }
}
