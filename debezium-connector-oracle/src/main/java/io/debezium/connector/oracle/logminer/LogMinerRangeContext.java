/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale.CURRENT_WRITE;
import static io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale.EXPONENTIAL;
import static io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBatchSizeWindowScale.LINEAR;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.Scn;

/**
 * A simple context that is responsible for computing the upper mining boundary based on the current read
 * position and the connector configuration. In addition, it maintains range details including the current
 * batch size and sleep time between each mining session.
 *
 * @author Chris Cranford
 */
public class LogMinerRangeContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerRangeContext.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;

    private int batchSize;
    private int ticks = 0;
    private long sleepTime;
    private Scn previousUpperBounds = Scn.NULL;

    public LogMinerRangeContext(OracleConnectorConfig connectorConfig, OracleConnection jdbcConnection, LogMinerStreamingChangeEventSourceMetrics metrics) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.metrics = metrics;

        this.batchSize = connectorConfig.getLogMiningBatchSizeDefault();
        this.sleepTime = connectorConfig.getLogMiningSleepTimeDefault().toMillis();

        this.metrics.setBatchSize(this.batchSize);
        this.metrics.setSleepTime(this.sleepTime);
    }

    /**
     * Calculates the upper mining boundary.
     *
     * @param lowerBoundary the lower boundary that we should read from, exclusive.
     * @param maximumArchiveLogScn the maximum scn in the archive logs, inclusive.
     * @param currentScn the maximum write position in the database, inclusive.
     * @return the upper boundary for the next mining step, inclusive
     * @throws SQLException when there is a problem communicating with the database
     */
    public Scn calculateUpperBoundary(Scn lowerBoundary, Scn maximumArchiveLogScn, Scn currentScn) throws SQLException {
        final Scn maximumReadScn = getMaximumReadScn(maximumArchiveLogScn, currentScn);

        // Initially set the upper bounds based on the current batchSize
        // The remaining logic will adjust this based on various criteria
        Scn upperBoundary = lowerBoundary.add(Scn.valueOf(batchSize));

        if (upperBoundary.compareTo(maximumReadScn) >= 0) {
            // The upper boundary is greater than the current write position of the database.
            // Given that reads cannot exceed the last write position, the cap to maximumReadScn.
            upperBoundary = maximumReadScn;

            // For next mining session, the mining step will have a smaller batch size
            // This is because we are now scaling beyond the current write position and the connector has caught up.
            decrementBatchSize();
            incrementSleepTime();

            // At this point the connector has caught up, it's safe to reset all window scale state
            ticks = 0;

            metrics.setBatchSize(batchSize);
        }
        else {
            // The upper boundary is still unsatisfactory to the current write position of the database.
            // If possible, increment the batchSize for the next mining step.
            incrementBatchSize();

            if (!previousUpperBounds.isNull() && isUsingMaxBatchSize()) {
                if (CURRENT_WRITE == connectorConfig.getLogMiningBatchSizeWindowScale()) {
                    LOGGER.debug("Using current write jump strategy, upper boundary is now {}.", maximumReadScn);
                    upperBoundary = maximumReadScn;

                    setMetricsBatchSizeFromBoundary(lowerBoundary, upperBoundary);
                }
                else {
                    final int effectiveBatchSize = updateAndGetEffectiveBatchSize();
                    upperBoundary = lowerBoundary.add(Scn.valueOf(effectiveBatchSize));

                    if (upperBoundary.compareTo(maximumReadScn) >= 0) {
                        upperBoundary = maximumReadScn;
                        setMetricsBatchSizeFromBoundary(lowerBoundary, upperBoundary);
                    }
                }
            }
            else {
                decrementSleepTime();
            }
        }

        // When the connector is configured with SCN deviation, this applies a sliding time window to the
        // computed upper boundary so that we are always the deviation time behind.
        final Duration deviationTime = connectorConfig.getLogMiningMaxScnDeviation();
        if (!deviationTime.isZero()) {
            final Optional<Scn> deviatedScn = calculateDeviatedEndScn(lowerBoundary, upperBoundary, deviationTime);
            if (deviatedScn.isPresent()) {
                LOGGER.debug("Adjusting upper boundary {} based on deviation to {}.", upperBoundary, deviatedScn.get());
                upperBoundary = deviatedScn.get();
            }
            else {
                LOGGER.warn(
                        "Failed to resolve upper boundary with '{}' because the SCN {} is no longer in undo retention. " +
                                "If this continues, consider removing the connector configuration property '{}'.",
                        OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS.name(),
                        upperBoundary,
                        OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS.name());
            }
        }

        upperBoundary = getMinimumOpenRedoThreadScn(lowerBoundary, upperBoundary);
        if (upperBoundary.isNull()) {
            // There was an issue resolving the minimum flushed SCN state, so the connector should return
            // Scn.NULL, which is a special sentinel case for it to pause and retry.
            return Scn.NULL;
        }

        // Final sanity check to avoid ORA-01281: SCN range specified is invalid.
        if (upperBoundary.compareTo(lowerBoundary) <= 0) {
            LOGGER.debug("Final upper boundary {} matches the starting lower boundary, delay required.", upperBoundary);
            return Scn.NULL;
        }

        LOGGER.debug("Resolved upper boundary as {}.", upperBoundary);
        previousUpperBounds = upperBoundary;
        return upperBoundary;
    }

    private boolean isUsingMaxBatchSize() {
        return connectorConfig.getLogMiningBatchSizeMax() == batchSize;
    }

    private void decrementBatchSize() {
        int batchSizeMin = connectorConfig.getLogMiningBatchSizeMin();
        if (batchSize > batchSizeMin) {
            batchSize = Math.max(batchSize - connectorConfig.getLogMiningBatchSizeIncrement(), batchSizeMin);
        }
    }

    private void incrementBatchSize() {
        int batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();
        if (batchSize < batchSizeMax) {
            batchSize = Math.min(batchSize + connectorConfig.getLogMiningBatchSizeIncrement(), batchSizeMax);
        }
    }

    private void incrementSleepTime() {
        final long sleepTimeMax = connectorConfig.getLogMiningSleepTimeMax().toMillis();
        final long sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();
        if (sleepTime < sleepTimeMax) {
            final long previousSleepTime = sleepTime;
            sleepTime = Math.min(sleepTime + sleepTimeIncrement, sleepTimeMax);
            if (previousSleepTime != sleepTime) {
                metrics.setSleepTime(sleepTime);
            }
        }
    }

    private void decrementSleepTime() {
        final long sleepTimeMin = connectorConfig.getLogMiningSleepTimeMin().toMillis();
        final long sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();
        if (sleepTime > sleepTimeMin) {
            final long previousSleepTime = sleepTime;
            sleepTime = Math.max(sleepTime - sleepTimeIncrement, sleepTimeMin);
            if (previousSleepTime != sleepTime) {
                metrics.setSleepTime(sleepTime);
            }
        }
    }

    private int updateAndGetEffectiveBatchSize() {
        final int batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();

        // This is limited to a max of 30 ticks to avoid bit shift beyond long when using exponential
        ticks = Math.min(ticks + 1, 30);

        int effectiveBatchSize = batchSize;
        if (LINEAR == connectorConfig.getLogMiningBatchSizeWindowScale()) {
            effectiveBatchSize = batchSize + (batchSizeMax * ticks);
        }
        else if (EXPONENTIAL == connectorConfig.getLogMiningBatchSizeWindowScale()) {
            effectiveBatchSize = (int) Math.min((long) batchSizeMax << ticks, Integer.MAX_VALUE);
        }

        metrics.setBatchSize(effectiveBatchSize);
        return effectiveBatchSize;
    }

    private void setMetricsBatchSizeFromBoundary(Scn lowerBoundary, Scn upperBoundary) {
        final BigInteger delta = upperBoundary.subtract(lowerBoundary).asBigInteger();
        if (BigInteger.valueOf(Integer.MAX_VALUE).compareTo(delta) > 0) {
            metrics.setBatchSize(delta.intValue());
        }
    }

    private Scn getMaximumReadScn(Scn maximumArchiveLogScn, Scn currentScn) {
        return connectorConfig.isArchiveLogOnlyMode() ? maximumArchiveLogScn : currentScn;
    }

    private Scn getMinimumOpenRedoThreadScn(Scn lowerBoundary, Scn upperBoundary) throws SQLException {
        Scn minimumScn = jdbcConnection.getRedoThreadState()
                .getThreads()
                .stream()
                .filter(RedoThreadState.RedoThread::isOpen)
                .map(RedoThreadState.RedoThread::getLastRedoScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);

        if (minimumScn.isNull()) {
            LOGGER.warn("There is no flushed redo thread data available, pausing...");
            return Scn.NULL;
        }

        // When using the last flushed SCN, the non-inclusive range will specify an SCN beyond what could be
        // in the logs. In addition, users may configure a larger SCN variance, so this adjusts the last
        // flushed SCN in V$THREAD by the configurable value, which defaults to 1.
        minimumScn = minimumScn.subtract(Scn.valueOf(connectorConfig.getLogMiningRedoThreadScnAdjustment()));

        // When there is a minimum SCN flushed to the V$$THREAD table, and the value is before the current
        // resolved upper boundary, the upper boundary should be capped
        if (minimumScn.compareTo(upperBoundary) < 0) {
            // There are corner cases where on the first start-up the lower boundary may be higher than the
            // last flushed SCN to V$THREAD if the snapshot completes too fast. In this case, the connector
            // should pause and wait for the next mining cycle.
            if (minimumScn.compareTo(lowerBoundary) < 0) {
                LOGGER.trace("The mining range low boundary {} has not been flushed to disk, pausing...", lowerBoundary);
                return Scn.NULL;
            }
            LOGGER.debug("Adjusting upper boundary {} to minimum flushed redo thread SCN {}.", upperBoundary, minimumScn);
            upperBoundary = minimumScn;
        }

        return upperBoundary;
    }

    /**
     * Calculates the deviated end scn based on the scn range and deviation.
     *
     * @param lowerBoundary the mining range's lower bounds
     * @param upperBoundary the mining range's upper bounds
     * @param deviation the time deviation
     * @return an optional that contains the deviated scn or empty if the operation should be performed again
     */
    private Optional<Scn> calculateDeviatedEndScn(Scn lowerBoundary, Scn upperBoundary, Duration deviation) {
        if (connectorConfig.isArchiveLogOnlyMode()) {
            // When archive-only mode is enabled, deviation should be ignored, even when enabled.
            return Optional.of(upperBoundary);
        }

        final Optional<Scn> calculatedDeviatedEndScn = getDeviatedMaxScn(upperBoundary, deviation);
        if (calculatedDeviatedEndScn.isEmpty() || calculatedDeviatedEndScn.get().isNull()) {
            // This happens only if the deviation calculation is outside the flashback/undo area or an exception was thrown.
            // In this case we have no choice but to use the upper bounds as a fallback.
            LOGGER.warn("Mining session end SCN deviation calculation is outside undo space, using upperbounds {}. If this continues, " +
                    "consider lowering the value of the '{}' configuration property.", upperBoundary,
                    OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS.name());
            return Optional.of(upperBoundary);
        }
        else if (calculatedDeviatedEndScn.get().compareTo(lowerBoundary) <= 0) {
            // This should also force the outer loop to recall this method again.
            LOGGER.debug("Mining session end SCN deviation as {}, outside of mining range, recalculating.", calculatedDeviatedEndScn.get());
            return Optional.empty();
        }
        else {
            // Calculated SCN is after lower bounds and within flashback/undo area, safe to return.
            return calculatedDeviatedEndScn;
        }
    }

    /**
     * Uses the provided Upperbound SCN and deviation to calculate an SCN that happened in the past at a
     * time based on Oracle's {@code TIMESTAMP_TO_SCN} and {@code SCN_TO_TIMESTAMP} functions.
     *
     * @param upperBoundary the upper bound system change number, should not be {@code null}
     * @param deviation the time deviation to be applied, should not be {@code null}
     * @return the newly calculated Scn
     */
    private Optional<Scn> getDeviatedMaxScn(Scn upperBoundary, Duration deviation) {
        try {
            final Scn currentScn = jdbcConnection.getCurrentScn();
            final Optional<Instant> currentInstant = jdbcConnection.getScnToTimestamp(currentScn);
            final Optional<Instant> upperInstant = jdbcConnection.getScnToTimestamp(upperBoundary);
            if (currentInstant.isPresent() && upperInstant.isPresent()) {
                // If the upper bounds satisfies the deviation time
                if (Duration.between(upperInstant.get(), currentInstant.get()).compareTo(deviation) >= 0) {
                    LOGGER.trace("Upper bounds {} is within deviation period, using it.", upperBoundary);
                    return Optional.of(upperBoundary);
                }
            }
            return Optional.of(jdbcConnection.getScnAdjustedByTime(upperBoundary, deviation));
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to calculate deviated max SCN value from {}.", upperBoundary);
            return Optional.empty();
        }
    }
}
