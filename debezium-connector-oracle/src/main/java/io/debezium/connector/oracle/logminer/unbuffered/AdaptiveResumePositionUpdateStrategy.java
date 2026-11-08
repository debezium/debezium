/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.util.Clock;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A {@link ResumePositionUpdateStrategy} that recalculates the resume position on a fixed interval, and
 * earlier when activity since the last recalculation reaches one of the configured thresholds.
 * <p>
 * The thresholds are:
 * <ul>
 *     <li>the number, or the combined size, of logs that hold changes written since the last recalculation</li>
 *     <li>the distance the commit system change number has moved since the last recalculation</li>
 *     <li>the number of transactions committed since the last recalculation</li>
 *     <li>the size of the largest transaction committed since the last recalculation</li>
 * </ul>
 * A threshold of {@code 0} is disabled. Thresholds never cause recalculations more often than the configured
 * minimum interval.
 * <p>
 * No recalculation happens while the commit position is unchanged since the last recalculation, because
 * the calculation only reads up to the commit position from the same resume position, and so it would
 * return the same result. The first recalculation is not subject to this rule, but it still waits for
 * the interval or a threshold, like any other recalculation.
 *
 * @author Chris Cranford
 */
public class AdaptiveResumePositionUpdateStrategy implements ResumePositionUpdateStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveResumePositionUpdateStrategy.class);

    private final Clock clock;
    private final Duration interval;
    private final Duration minInterval;
    private final long logCountThreshold;
    private final long logBytesThreshold;
    private final long scnDistanceThreshold;
    private final long commitsThreshold;
    private final long transactionEventsThreshold;

    private Scn lastCommitScn;
    private Timer intervalTimer;
    private Timer minIntervalTimer;
    private long commits;
    private long maxTransactionEvents;

    public AdaptiveResumePositionUpdateStrategy(OracleConnectorConfig connectorConfig, Clock clock) {
        this.clock = clock;
        this.interval = connectorConfig.getResumePositionUpdateInterval();
        this.minInterval = connectorConfig.getResumePositionMinUpdateInterval();
        this.logCountThreshold = connectorConfig.getResumePositionLogCountThreshold();
        this.logBytesThreshold = connectorConfig.getResumePositionLogBytesThreshold();
        this.scnDistanceThreshold = connectorConfig.getResumePositionScnDistanceThreshold();
        this.commitsThreshold = connectorConfig.getResumePositionCommitsThreshold();
        this.transactionEventsThreshold = connectorConfig.getResumePositionTransactionEventsThreshold();
        resetTimers();
    }

    @Override
    public void onCommit(int eventCount) {
        commits++;
        maxTransactionEvents = Math.max(maxTransactionEvents, eventCount);
    }

    @Override
    public boolean shouldUpdate(Scn resumeScn, Scn commitScn, List<LogFile> logFiles) {
        if (lastCommitScn != null && commitScn.equals(lastCommitScn)) {
            // Same inputs as the last calculation, the result would be identical
            return false;
        }

        if (!minIntervalTimer.expired()) {
            return false;
        }

        if (intervalTimer.expired()) {
            LOGGER.debug("Updating resume position, interval {} elapsed.", interval);
            return true;
        }

        // Log and SCN growth is measured from the last calculation, not from the resume position. A long-running
        // transaction can hold the resume position back, and measuring from it would then keep the threshold
        // reached after every calculation, even though the calculation cannot advance the resume position.
        final Scn baselineScn = getBaselineScn(resumeScn);

        if (logCountThreshold > 0 || logBytesThreshold > 0) {
            long logCount = 0;
            long logBytes = 0;
            for (LogFile logFile : logFiles) {
                final Scn nextScn = logFile.getNextScn();
                if (nextScn == null || nextScn.isNull() || nextScn.compareTo(baselineScn) > 0) {
                    logCount++;
                    logBytes += logFile.getBytes();
                }
            }
            if (logCountThreshold > 0 && logCount >= logCountThreshold) {
                LOGGER.debug("Updating resume position, {} logs after SCN {} reached threshold {}.",
                        logCount, baselineScn, logCountThreshold);
                return true;
            }
            if (logBytesThreshold > 0 && logBytes >= logBytesThreshold) {
                LOGGER.debug("Updating resume position, {} log bytes after SCN {} reached threshold {}.",
                        logBytes, baselineScn, logBytesThreshold);
                return true;
            }
        }

        if (scnDistanceThreshold > 0 && !baselineScn.isNull() && !commitScn.isNull()) {
            final Scn distance = commitScn.subtract(baselineScn);
            if (distance.compareTo(Scn.valueOf(scnDistanceThreshold)) >= 0) {
                LOGGER.debug("Updating resume position, SCN distance {} between SCN {} and commit SCN {} reached threshold {}.",
                        distance, baselineScn, commitScn, scnDistanceThreshold);
                return true;
            }
        }

        if (commitsThreshold > 0 && commits >= commitsThreshold) {
            LOGGER.debug("Updating resume position, {} commits reached threshold {}.", commits, commitsThreshold);
            return true;
        }

        if (transactionEventsThreshold > 0 && maxTransactionEvents >= transactionEventsThreshold) {
            LOGGER.debug("Updating resume position, transaction with {} events reached threshold {}.",
                    maxTransactionEvents, transactionEventsThreshold);
            return true;
        }

        return false;
    }

    @Override
    public void onUpdated(Scn commitScn) {
        lastCommitScn = commitScn;
        commits = 0;
        maxTransactionEvents = 0;
        resetTimers();
    }

    /**
     * Returns the position that log and SCN growth is measured from: the commit position of the last
     * calculation, or the resume position when that is further ahead or no calculation has happened yet.
     */
    private Scn getBaselineScn(Scn resumeScn) {
        if (lastCommitScn == null || lastCommitScn.isNull()) {
            return resumeScn;
        }
        if (resumeScn == null || resumeScn.isNull()) {
            return lastCommitScn;
        }
        return lastCommitScn.compareTo(resumeScn) > 0 ? lastCommitScn : resumeScn;
    }

    private void resetTimers() {
        intervalTimer = Threads.timer(clock, interval);
        minIntervalTimer = Threads.timer(clock, minInterval);
    }
}
