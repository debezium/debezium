/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.buffered.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.RacCommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.ReadOnlyLogWriterFlushStrategy;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Stopwatch;

/**
 * An implementation of {@link AbstractLogMinerStreamingChangeEventSource} to read changes from LogMiner
 * using uncommitted mode paired with heap and off-heap caches for in-flight transaction storage.
 * <p>
 * The event handler loop is executed in a separate executor.
 */
public class BufferedLogMinerStreamingChangeEventSource extends AbstractLogMinerStreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedLogMinerStreamingChangeEventSource.class);

    public BufferedLogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig,
                                                      OracleConnection jdbcConnection,
                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                      ErrorHandler errorHandler,
                                                      Clock clock,
                                                      OracleDatabaseSchema schema,
                                                      Configuration jdbcConfig,
                                                      LogMinerStreamingChangeEventSourceMetrics streamingMetrics) {
        super(connectorConfig, jdbcConnection, dispatcher, errorHandler, clock, schema, jdbcConfig, streamingMetrics);
    }

    @Override
    protected void executeLogMiningStreaming(ChangeEventSourceContext context, OraclePartition partition, OracleOffsetContext offsetContext)
            throws Exception {

        try (LogWriterFlushStrategy flushStrategy = resolveFlushStrategy()) {
            try (LogMinerEventProcessor processor = createProcessor(context)) {

                Scn startScn = getOffsetContext().getScn();
                Scn endScn = Scn.NULL;

                Stopwatch watch = Stopwatch.accumulating().start();
                int miningStartAttempts = 1;

                prepareLogsForMining(false, startScn);

                while (context.isRunning()) {

                    // Check if we should break when using archive log only mode
                    if (isArchiveLogOnlyModeAndScnIsNotAvailable(context, startScn)) {
                        break;
                    }

                    final Instant batchStartTime = Instant.now();

                    updateDatabaseTimeDifference();

                    Scn currentScn = getCurrentScn();
                    getMetrics().setCurrentScn(currentScn);

                    endScn = calculateUpperBounds(startScn, endScn, currentScn);
                    if (endScn.isNull()) {
                        LOGGER.debug("Requested delay of mining by one iteration");
                        pauseBetweenMiningSessions();
                        continue;
                    }

                    // This is a small window where when archive log only mode has completely caught up to the last
                    // record in the archive logs that both the lower and upper values are identical. In this use
                    // case we want to pause and restart the loop waiting for a new archive log before proceeding.
                    if (getConfig().isArchiveLogOnlyMode() && startScn.equals(endScn)) {
                        pauseBetweenMiningSessions();
                        continue;
                    }

                    flushStrategy.flush(getCurrentScn());

                    if (isMiningSessionRestartRequired(watch) || checkLogSwitchOccurredAndUpdate()) {
                        // Mining session is active, so end the current session and restart if necessary
                        endMiningSession();
                        if (getConfig().isLogMiningRestartConnection()) {
                            prepareJdbcConnection(true);
                        }

                        prepareLogsForMining(true, startScn);

                        // Recreate the stop watch
                        watch = Stopwatch.accumulating().start();
                    }

                    if (startMiningSession(startScn, endScn, miningStartAttempts)) {
                        miningStartAttempts = 1;
                        startScn = processor.process(startScn, endScn);

                        getMetrics().setLastBatchProcessingDuration(Duration.between(batchStartTime, Instant.now()));
                    }
                    else {
                        miningStartAttempts++;
                    }

                    captureJdbcSessionMemoryStatistics();

                    pauseBetweenMiningSessions();

                    if (context.isPaused()) {
                        LOGGER.info("Streaming will now pause");
                        context.streamingPaused();
                        context.waitSnapshotCompletion();
                        LOGGER.info("Streaming resumed");
                    }
                }
            }
        }
    }

    private LogMinerEventProcessor createProcessor(ChangeEventSourceContext context) {
        return getConfig().getLogMiningBufferType().createProcessor(context,
                getConfig(),
                getConnection(),
                getEventDispatcher(),
                getPartition(),
                getOffsetContext(),
                getSchema(),
                getMetrics());
    }

    /**
     * Resolves the Oracle LGWR buffer flushing strategy.
     *
     * @return the strategy to be used to flush Oracle's LGWR process, never {@code null}.
     */
    private LogWriterFlushStrategy resolveFlushStrategy() {
        if (getConfig().isLogMiningReadOnly()) {
            return new ReadOnlyLogWriterFlushStrategy();
        }
        if (getConfig().isRacSystem()) {
            return new RacCommitLogWriterFlushStrategy(getConfig(), getJdbcConfiguration(), getMetrics());
        }
        return new CommitLogWriterFlushStrategy(getConfig(), getConnection());
    }

}
