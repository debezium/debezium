/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.function.Predicates.not;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.RedoThreadState;
import io.debezium.connector.oracle.RedoThreadState.RedoThread;
import io.debezium.connector.oracle.Scn;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Strings;

/**
 * A collector that is responsible for fetching, deduplication, and supplying Debezium with a set of
 * {@link LogFile} instances that should be mined given a specific {@link Scn}.
 *
 * @author Chris Cranford
 */
public class LogFileCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogFileCollector.class);
    private static final String STATUS_CURRENT = "CURRENT";
    private static final String ONLINE_LOG_TYPE = "ONLINE";
    private static final String ARCHIVE_LOG_TYPE = "ARCHIVED";

    private final Duration initialDelay;
    private final Duration maxRetryDelay;
    private final int maxAttempts;
    private final Duration archiveLogRetention;
    private final boolean archiveLogOnlyMode;
    private final String archiveLogDestinationName;
    private final OracleConnection connection;

    public LogFileCollector(OracleConnectorConfig connectorConfig, OracleConnection connection) {
        this.initialDelay = connectorConfig.getLogMiningInitialDelay();
        this.maxRetryDelay = connectorConfig.getLogMiningMaxDelay();
        this.maxAttempts = connectorConfig.getMaximumNumberOfLogQueryRetries();
        this.archiveLogRetention = connectorConfig.getArchiveLogRetention();
        this.archiveLogOnlyMode = connectorConfig.isArchiveLogOnlyMode();
        this.archiveLogDestinationName = connectorConfig.getArchiveLogDestinationName();
        this.connection = connection;
    }

    /**
     * Get a list of all log files that should be mined given the specified system change number.
     *
     * @param offsetScn minimum system change number to start reading changes from, should not be {@code null}
     * @return list of log file instances that should be added to the mining session, never {@code null}
     * @throws SQLException if there is a database failure during the collection
     * @throws LogFileNotFoundException if we were unable to collect logs due to a non-SQL related failure
     */
    public List<LogFile> getLogs(Scn offsetScn) throws SQLException, LogFileNotFoundException {
        LOGGER.debug("Collecting logs based on the read SCN position {}.", offsetScn);
        final DelayStrategy retryStrategy = DelayStrategy.exponential(initialDelay, maxRetryDelay);
        for (int attempt = 0; attempt <= maxAttempts; ++attempt) {
            // Fetch current Redo Thread State
            final RedoThreadState currentRedoThreadState = connection.getRedoThreadState();
            for (RedoThread redoThread : currentRedoThreadState.getThreads()) {
                LOGGER.debug("Thread {}: {}", redoThread.getThreadId(), redoThread);
            }

            // Fetch logs
            final List<LogFile> files = getLogsForOffsetScn(offsetScn);
            if (!isLogFileListConsistent(offsetScn, files, currentRedoThreadState)) {
                LOGGER.info("No logs available yet (attempt {})...", attempt + 1);
                retryStrategy.sleepWhen(true);
                continue;
            }

            return files;
        }
        throw new LogFileNotFoundException(offsetScn);
    }

    @VisibleForTesting
    public List<LogFile> getLogsForOffsetScn(Scn offsetScn) throws SQLException {
        final Set<LogFile> onlineRedoLogs = new LinkedHashSet<>();
        final Set<LogFile> archiveLogs = new LinkedHashSet<>();

        connection.query(getLogsQuery(offsetScn), rs -> {
            while (rs.next()) {
                final String fileName = rs.getString(1);
                final Scn firstScn = getScnFromString(rs.getString(2));
                final Scn nextScn = getScnFromString(rs.getString(3));
                final String status = rs.getString(5);
                final String type = rs.getString(6);
                final BigInteger sequence = new BigInteger(rs.getString(7));
                final int thread = rs.getInt(10);
                if (ARCHIVE_LOG_TYPE.equals(type)) {
                    final LogFile log = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE, thread);
                    if (log.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.debug("Archive log {} with SCN range {} to {} sequence {} to be added.",
                                fileName, firstScn, nextScn, sequence);
                        archiveLogs.add(log);
                    }
                }
                else if (ONLINE_LOG_TYPE.equals(type)) {
                    final LogFile log = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO,
                            STATUS_CURRENT.equalsIgnoreCase(status), thread);
                    if (log.isCurrent() || log.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.debug("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.",
                                fileName, firstScn, nextScn, status, sequence);
                        onlineRedoLogs.add(log);
                    }
                    else {
                        LOGGER.debug("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.",
                                fileName, firstScn, nextScn, status, sequence);
                    }
                }
            }
        });

        return deduplicateLogFiles(archiveLogs, onlineRedoLogs);
    }

    @VisibleForTesting
    public List<LogFile> deduplicateLogFiles(Collection<LogFile> archiveLogFiles, Collection<LogFile> onlineLogFiles) {
        // DBZ-3563
        // To avoid duplicate log files (ORA-01289 cannot add duplicate logfile)
        // Remove the archive log which has the same sequence number and redo thread number.
        for (LogFile redoLog : onlineLogFiles) {
            archiveLogFiles.removeIf(archiveLog -> {
                if (archiveLog.equals(redoLog)) {
                    LOGGER.debug("Removing redo thread {} archive log {} with duplicate sequence {} with redo log {}",
                            archiveLog.getThread(), archiveLog.getFileName(), archiveLog.getSequence(), redoLog.getFileName());
                    return true;
                }
                return false;
            });
        }

        final List<LogFile> allLogs = new ArrayList<>();
        allLogs.addAll(archiveLogFiles);
        allLogs.addAll(onlineLogFiles);
        return allLogs;
    }

    /**
     * Checks consistency of the list of log files for redo threads in the {@code currentRedoThreadState} state.
     *
     * @param startScn the read position system change number, should not be {@code null}
     * @param logs the list of logs to inspect, should not be {@code null}
     * @param currentRedoThreadState the current database redo thread state, should not be {@code null}
     * @return {@code true} if the logs are consistent; {@code false} otherwise
     */
    @VisibleForTesting
    public boolean isLogFileListConsistent(Scn startScn, List<LogFile> logs, RedoThreadState currentRedoThreadState) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Performing consistency check on the following collected logs:");
            for (LogFile logFile : logs) {
                LOGGER.debug("\tLog: {}", logFile);
            }

            LOGGER.debug("Current redo thread state:");
            for (RedoThread redoThread : currentRedoThreadState.getThreads()) {
                LOGGER.debug("\tThread: {}", redoThread);
            }
        }

        // Generate a map of the logs by redo thread
        final Map<Integer, List<LogFile>> redoThreadLogs = logs.stream()
                .collect(Collectors.groupingBy(LogFile::getThread));

        final List<Integer> currentThreads = currentRedoThreadState.getThreads()
                .stream()
                .map(RedoThread::getThreadId)
                .collect(Collectors.toList());

        // Checks current redo thread state against the logs
        for (Integer threadId : currentThreads) {
            final RedoThread redoThread = currentRedoThreadState.getRedoThread(threadId);
            if (redoThread.isOpen()) {
                if (!isOpenThreadConsistent(redoThread, startScn, redoThreadLogs.get(threadId))) {
                    return false;
                }
            }
            else {
                if (!isClosedThreadConsistent(redoThread, startScn, redoThreadLogs.get(threadId))) {
                    return false;
                }
            }
        }

        // This collection should ideally generate no results
        // Collecting these to output debug entries should there be a mismatch
        logs.stream()
                .map(LogFile::getThread)
                .filter(not(currentThreads::contains))
                .forEach(this::logThreadCheckSkippedNotInDatabase);

        return true;
    }

    /**
     * Checks whether the specified open {@code thread} has consistent redo-logs in the specified collection.
     *
     * @param thread the redo thread to inspect; should not be {@code null}
     * @param startScn the read position system change number, should not be {@code null}
     * @param threadLogs the redo-thread logs to check consistency against, may be {@code null} or empty
     * @return {@code true} if the open thread's logs are consistent; {@code false} otherwise
     */
    private boolean isOpenThreadConsistent(RedoThread thread, Scn startScn, List<LogFile> threadLogs) {
        final int threadId = thread.getThreadId();
        final Scn enabledScn = thread.getEnabledScn();
        final Scn checkpointScn = thread.getCheckpointScn();

        if (thread.isDisabled()) {
            logException(String.format("Redo thread %d expected to have ENABLED with value PUBLIC or PRIVATE.", threadId));
            return false;
        }

        if (threadLogs == null || threadLogs.isEmpty()) {
            logException(String.format("Redo thread %d is inconsistent; enabled SCN %s checkpoint SCN %s reading from SCN %s, no logs found.",
                    threadId, enabledScn, checkpointScn, startScn));
            return false;
        }

        // Consistency is expected since the ENABLED_SCN point.
        if (enabledScn.compareTo(startScn) > 0) {
            // Thread was enabled after the read position
            // Consistency should only be applied for logs including or that come after the enabledScn
            final List<LogFile> enabledLogs = threadLogs.stream()
                    .filter(log -> log.isScnInLogFileRange(enabledScn) || log.getFirstScn().compareTo(enabledScn) > 0)
                    .collect(Collectors.toList());

            if (enabledLogs.isEmpty()) {
                logException(String.format("Redo Thread %d is inconsistent; expected logs after enabled SCN %s",
                        threadId, enabledLogs));
                return false;
            }

            final Optional<Long> missingSequence = getFirstLogMissingSequence(enabledLogs);
            if (missingSequence.isPresent()) {
                logException(String.format("Redo Thread %d is inconsistent; failed to find log with sequence %d (enabled).",
                        threadId, missingSequence.get()));
                return false;
            }

            LOGGER.debug("Redo Thread {} is consistent after enabled SCN {} ({}).", threadId, enabledScn, thread.getStatus());
        }
        else {
            // Check whether the redo logs have the read position
            if (threadLogs.stream().noneMatch(log -> log.isScnInLogFileRange(startScn))) {
                try {
                    // Collect all archive logs for a given redo thread that has an SCN range that comes
                    // after or includes the startScn plus the archive log that has a range that comes
                    // immediately before the startScn
                    final List<LogFile> allThreadArchiveLogs = getAllRedoThreadArchiveLogs(threadId);

                    if (allThreadArchiveLogs.isEmpty()) {
                        logException(String.format("Redo Thread %d is inconsistent; at least one archive log expected.", threadId));
                        return false;
                    }
                    else if (allThreadArchiveLogs.stream().anyMatch(l -> l.isScnInLogFileRange(startScn))) {
                        logException(String.format("Redo thread %d is inconsistent; does not have a log that conatins scn %s. " +
                                "A recent log switch may not have been archived by the Oracle ARC process yet.", threadId, startScn));
                        return false;
                    }
                    else {
                        // Collects all archive logs for a given redo thread and returns the first archive log
                        // with an SCN range that comes immediately before the log with a range that includes the startScn.
                        // As the query is specific to a singular archive log destination, there should always either
                        // be one log or no log at all that matches this criteria.
                        final Optional<LogFile> logWithRangeBeforeStartScn = allThreadArchiveLogs.stream()
                                .filter(l -> l.getFirstScn().compareTo(startScn) < 0 && !l.isScnInLogFileRange(startScn))
                                .findFirst();

                        // We expect that there should be an archive log with a range just before startScn
                        // If this doesn't exist, we cannot guarantee there is not a gap in archive log sequences next
                        if (logWithRangeBeforeStartScn.isEmpty()) {
                            logException(String.format("Redo Thread %d is inconsistent; expected archive log with range just before scn %s.",
                                    threadId, startScn));
                            return false;
                        }

                        final Optional<Long> missingSequence = getFirstLogMissingSequence(Stream.concat(
                                threadLogs.stream(), logWithRangeBeforeStartScn.stream()).toList());
                        if (missingSequence.isPresent()) {
                            logException(String.format("Redo Thread %d is inconsistent; an archive log with sequence %d is not available",
                                    threadId, missingSequence.get()));
                            return false;
                        }
                    }

                    // Reaching here means that the log set for mining have scn ranges that come after the startScn.
                    // Since the curated log set of archive logs and the mined thread logs have no sequence gaps,
                    // the thread is consistent and represents a recently opened redo thread that was previously
                    // closed. The startScn in this case refers to a position in the archive logs where the thread
                    // was currently closed, but due to no sequence gaps, thread can be treated as consistent.
                }
                catch (SQLException e) {
                    logException(String.format("Redo thread %d is inconsistent; " + e.getMessage(), threadId), e);
                    return false;
                }
            }

            // Make sure the thread logs from the read position until now have no gaps
            final Optional<Long> missingSequence = getFirstLogMissingSequence(threadLogs);
            if (missingSequence.isPresent()) {
                logException(String.format("Redo Thread %d is inconsistent; failed to find log with sequence %d",
                        threadId, missingSequence.get()));
                return false;
            }

            LOGGER.debug("Redo Thread {} is consistent.", threadId);
        }
        return true;
    }

    /**
     * Checks whether the specified closed {@code thread} has consistent redo-logs in the specified collection.
     *
     * @param thread the redo thread to inspect; should not be {@code null}
     * @param startScn the read position system change number, should not be {@code null}
     * @param threadLogs the redo-thread logs to check consistency against, may be {@code null} or empty
     * @return {@code true} if the closed thread's logs are consistent; {@code false} otherwise
     */
    private boolean isClosedThreadConsistent(RedoThread thread, Scn startScn, List<LogFile> threadLogs) {
        final int threadId = thread.getThreadId();
        if (!thread.isDisabled()) {
            // The node was shutdown and not disabled.
            // Consistency check should be based on the last flushed SCN to disk, CHECKPOINT_SCN.
            final Scn checkpointScn = thread.getCheckpointScn();
            final Scn enabledScn = thread.getEnabledScn();
            if (threadLogs != null && !threadLogs.isEmpty()) {
                if (checkpointScn.compareTo(startScn) < 0) {
                    // Node was shutdown before the read position.
                    // There should be no logs in this case; likely indicates a query failure.
                    if (LOGGER.isDebugEnabled()) {
                        for (LogFile logFile : threadLogs) {
                            LOGGER.debug("Read Thread {} query has log {}; not expected.", threadId, logFile);
                        }
                    }
                    logException(String.format("Redo Thread %d stopped at SCN %s, but logs detected using SCN %s.",
                            threadId, checkpointScn, startScn));
                    return false;
                }

                final List<LogFile> logsToCheck;
                if (enabledScn.compareTo(startScn) > 0) {
                    // The thread was recently added but is in a closed state.
                    // Log consistency check should be from ENABLED_SCN to CHECKPOINT_SCN
                    logsToCheck = threadLogs.stream()
                            .filter(log -> log.isScnInLogFileRange(enabledScn) || log.getNextScn().compareTo(enabledScn) >= 0)
                            .filter(log -> log.isScnInLogFileRange(checkpointScn) || log.getFirstScn().compareTo(checkpointScn) < 0)
                            .collect(Collectors.toList());

                    if (logsToCheck.isEmpty()) {
                        logException(String.format("Redo Thread %d is inconsistent; expected logs between enabled SCN %s and checkpoint SCN %s",
                                threadId, enabledScn, checkpointScn));
                        return false;
                    }
                }
                else {
                    // The thread was enabled (added) before the read position
                    // Log consistency check should be to the CHECKPOINT_SCN
                    logsToCheck = threadLogs.stream()
                            .filter(log -> log.isScnInLogFileRange(checkpointScn) || log.getFirstScn().compareTo(checkpointScn) < 0)
                            .collect(Collectors.toList());

                    if (logsToCheck.isEmpty()) {
                        logException(String.format("Redo Thread %d is inconsistent; expected logs before checkpoint SCN %s",
                                threadId, checkpointScn));
                        return false;
                    }
                }

                final Optional<Long> missingSequence = getFirstLogMissingSequence(logsToCheck);
                if (missingSequence.isPresent()) {
                    logException(String.format("Redo Thread %d is inconsistent; failed to find log with sequence %d (checkpoint).",
                            threadId, missingSequence.get()));
                    return false;
                }
            }
            LOGGER.debug("Redo Thread {} is consistent before checkpoint SCN {} ({}).", threadId, checkpointScn, thread.getStatus());
        }
        else {
            // The node is active but disabled.
            // Consistency check should be based on the DISABLED_SCN, if one exists.
            final Scn disabledScn = thread.getDisabledScn();
            if (disabledScn.isNull() || disabledScn.asBigInteger().equals(BigInteger.ZERO)) {
                LOGGER.debug("Redo Thread {} is disabled but has no disabled SCN; consistency check skipped.", threadId);
                return true;
            }

            // If there are logs we need to check the consistency state up to the DISABLED_SCN
            if (threadLogs != null && !threadLogs.isEmpty()) {
                if (disabledScn.compareTo(startScn) < 0) {
                    // Thread was disabled before the read position; there should be no logs in this case.
                    // This likely indicates a query failure.
                    if (LOGGER.isDebugEnabled()) {
                        for (LogFile log : threadLogs) {
                            LOGGER.debug("Redo Thread {} log {} not expected.", threadId, log);
                        }
                    }
                    logException(String.format("Redo Thread %d disabled at SCN %s, but logs detected using SCN %s.",
                            threadId, disabledScn, startScn));
                    return false;
                }

                // Consistency is expected up to the DISABLED_SCN point.
                final List<LogFile> disabledLogs = threadLogs.stream()
                        .filter(log -> log.isScnInLogFileRange(disabledScn) || log.getFirstScn().compareTo(disabledScn) < 0)
                        .collect(Collectors.toList());

                if (disabledLogs.isEmpty()) {
                    logException(String.format("Redo Thread %d is inconsistent; expected logs before disabled SCN %s.",
                            threadId, disabledLogs));
                    return false;
                }

                final Optional<Long> missingSequence = getFirstLogMissingSequence(disabledLogs);
                if (missingSequence.isPresent()) {
                    logException(String.format("Redo Thread %d is inconsistent; failed to find log with sequence %d.",
                            threadId, missingSequence.get()));
                    return false;
                }
            }
            LOGGER.debug("Redo Thread {} is consistent after disabled SCN {} ({}).", threadId, disabledScn, thread.getStatus());
        }

        return true;
    }

    /**
     * Searches the specified redo-thread logs and returns the first missing log sequence; if any exist.
     * This method will return an empty value if there are no sequence gaps.
     *
     * @param logFiles the redo-thread logs to search; should not be {@code null} or empty
     * @return the first missing sequence or an empty value if no gaps are detected
     */
    private Optional<Long> getFirstLogMissingSequence(List<LogFile> logFiles) {
        final SequenceRange range = getSequenceRangeForRedoThreadLogs(logFiles);
        for (long sequence = range.getMin(); sequence <= range.getMax(); sequence++) {
            if (!hasLogFileWithSequenceNumber(sequence, logFiles)) {
                return Optional.of(sequence);
            }
        }
        return Optional.empty();
    }

    /**
     * Get all archive logs for a given redo thread, ordered by sequence in descending order.
     *
     * @param threadId the redo thread id
     * @return all available archive logs for the given redo thread
     * @throws SQLException if a database exception is thrown
     */
    @VisibleForTesting
    public List<LogFile> getAllRedoThreadArchiveLogs(int threadId) throws SQLException {
        return connection.queryAndMap(
                SqlUtils.allRedoThreadArchiveLogs(threadId, archiveLogDestinationName),
                rs -> {
                    final List<LogFile> logs = new ArrayList<>();
                    while (rs.next()) {
                        logs.add(new LogFile(
                                rs.getString(1),
                                Scn.valueOf(rs.getString(3)),
                                Scn.valueOf(rs.getString(4)),
                                BigInteger.valueOf(rs.getLong(2)),
                                LogFile.Type.ARCHIVE,
                                threadId));
                    }
                    return logs;
                });
    }

    /**
     * Logs a warning that the thread check was skipped because the specified thread has no matching {@code V$THREAD} entry.
     *
     * @param threadId the redo thread id with no matching database record.
     */
    private void logThreadCheckSkippedNotInDatabase(int threadId) {
        LOGGER.warn("Log found for redo thread {} but no record in V$THREAD; thread consistency check skipped.", threadId);
    }

    /**
     * Get the SQL query to fetch logs that contain or come after the specified system change number.
     *
     * @param offsetScn the starting system change number to read from, should not be {@code null}
     * @return query string
     */
    private String getLogsQuery(Scn offsetScn) {
        return SqlUtils.allMinableLogsQuery(offsetScn, archiveLogRetention, archiveLogOnlyMode, archiveLogDestinationName);
    }

    /**
     * Converts the specified string {@code value} to an {@link Scn}.
     *
     * @param value the value to convert, can be {@code null} or empty
     * @return the system change number for the specified value
     */
    private Scn getScnFromString(String value) {
        return Strings.isNullOrBlank(value) ? Scn.MAX : Scn.valueOf(value);
    }

    /**
     * Check whether there is a log file in the collection with the specified sequence number.
     *
     * @param sequenceId the sequence number to check
     * @param redoThreadLogs the collection of redo-thread specific logs to inspect
     * @return {@code true} if a log exists with the sequence; {@code false} otherwise
     */
    private boolean hasLogFileWithSequenceNumber(long sequenceId, List<LogFile> redoThreadLogs) {
        return redoThreadLogs.stream().map(LogFile::getSequence).anyMatch(sequence -> sequence.longValue() == sequenceId);
    }

    /**
     * Calculates the sequence range for a collection of logs for a given redo thread.
     *
     * @param redoThreadLogs the redo logs collection, should not be {@code empty}.
     * @return the sequence range for the collection of logs
     * @throws DebeziumException if the log collection is empty
     */
    private SequenceRange getSequenceRangeForRedoThreadLogs(List<LogFile> redoThreadLogs) {
        if (redoThreadLogs.isEmpty()) {
            throw new DebeziumException("Cannot calculate log sequence range, log collection is empty.");
        }

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (LogFile logFile : redoThreadLogs) {
            min = Math.min(logFile.getSequence().longValue(), min);
            max = Math.max(logFile.getSequence().longValue(), max);
        }
        return new SequenceRange(min, max);
    }

    /**
     * Get the minimum sequence from a list of redo thread logs.
     *
     * @param redoThreadLogs the redo logs collection, should not be {@code empty} or {@code null}.
     * @return the minimum sequence
     */
    private BigInteger getMinRedoThreadLogSequence(List<LogFile> redoThreadLogs) {
        if (redoThreadLogs == null || redoThreadLogs.isEmpty()) {
            throw new DebeziumException("Cannot calculate minimum sequence on a null or empty list of logs");
        }
        return redoThreadLogs.stream().map(LogFile::getSequence).min(BigInteger::compareTo).get();
    }

    private static void logException(String message) {
        LOGGER.info("{}", message, new DebeziumException(message));
    }

    private static void logException(String message, Throwable cause) {
        LOGGER.info("{}", message, new DebeziumException(message, cause));
    }

    /**
     * Represents an inclusive range between two values.
     */
    @Immutable
    private static class SequenceRange {
        private final long min;
        private final long max;

        SequenceRange(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }

}
