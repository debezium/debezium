/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.RedoThreadState.RedoThread;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFileCollector.LogFilesResult;

/**
 * A LogMiner log file selector that caps the returned logs based on user configuration, while capping the
 * mining session window upper boundary to the minimum upper system change number across all threads.
 *
 * @author Chris Cranford
 */
public class CappedLogFileSessionSelector implements LogFileSessionSelector {

    private final Logger LOGGER = LoggerFactory.getLogger(CappedLogFileSessionSelector.class);

    private final int minimumLogsPerRedoThread;
    private final long redoLogSizeInBytes;

    private int logsPerRedoThread;
    private Map<Integer, List<LogFile>> previousCappedLogsByThread;
    private Scn previousEffectiveUpperBoundary;

    public CappedLogFileSessionSelector(int minimumLogsPerRedoThread, long redoLogSizeInBytes) {
        this.minimumLogsPerRedoThread = minimumLogsPerRedoThread;
        this.redoLogSizeInBytes = redoLogSizeInBytes;
        this.logsPerRedoThread = minimumLogsPerRedoThread;
    }

    @Override
    public SessionLogSelection selectLogsForSession(LogFilesResult logFilesResult, Scn upperBoundary) {
        Scn effectiveUpperBoundary = upperBoundary;

        // Groups all collected logs by redo thread, sorted in ascending order by sequence.
        // The ordering is important for this algorithm when inspecting what is the first/last logs per thread.
        final Map<Integer, List<LogFile>> logsByThread = logFilesResult.logFiles().stream()
                .sorted(Comparator.comparing(LogFile::getSequence))
                .collect(Collectors.groupingBy(LogFile::getThread));

        Map<Integer, List<LogFile>> cappedLogsByThread = getThreadLogsCappedBySize(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);

        if (previousCappedLogsByThread != null) {
            if (cappedLogsByThread.equals(previousCappedLogsByThread)) {
                // Same log set as last iteration: the lower watermark did not advance, so a long-running
                // transaction may extend beyond the current cap. Grow by one log to find the end.
                logsPerRedoThread++;
                LOGGER.debug("Capped log set unchanged, growing log count per redo thread to {}.", logsPerRedoThread);
                cappedLogsByThread = getThreadLogsCappedBySize(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);
            }
            else if (logsPerRedoThread > minimumLogsPerRedoThread) {
                // Log set changed: the watermark advanced, so reset the count back to the configured minimum.
                logsPerRedoThread = minimumLogsPerRedoThread;
                LOGGER.debug("Capped log set changed, resetting log count per redo thread to {}.", logsPerRedoThread);
                cappedLogsByThread = getThreadLogsCappedBySize(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);
            }
        }

        previousCappedLogsByThread = cappedLogsByThread;

        boolean allThreadsMineOnline = true;
        for (RedoThread redoThread : logFilesResult.redoThreadState().getThreads()) {
            if (redoThread.isOpen()) {
                final List<LogFile> threadLogs = cappedLogsByThread.get(redoThread.getThreadId());
                if (threadLogs == null) {
                    // Should never happen, just sanity check
                    throw new DebeziumException("Redo thread %d is open, expected logs".formatted(redoThread.getThreadId()));
                }

                // Checks if the last log in the thread's capped list is an online redo log.
                // When all redo threads are capped to the online redo, we handle this differently.
                final LogFile lastThreadLog = threadLogs.get(threadLogs.size() - 1);
                if (!lastThreadLog.isCurrent()) {
                    allThreadsMineOnline = false;

                    // When last log is an archive, cap the upper boundary to the logs next scn, but
                    // only if its next scn value is less than the current effective upper boundary.
                    // This guarantees we get the smallest upper position across all threads.
                    final Scn lastLogNextScn = lastThreadLog.getNextScn();
                    if (lastLogNextScn.compareTo(effectiveUpperBoundary) < 0) {
                        effectiveUpperBoundary = lastLogNextScn;
                    }
                }
            }
        }

        if (allThreadsMineOnline) {
            LOGGER.debug("All threads are reading online redo, using all logs and reading up to {}.", upperBoundary);
            // When all threads mine online redo logs, no upper boundary cap is necessary
            // Resort the log files in thread+sequence order for application.
            previousEffectiveUpperBoundary = upperBoundary;
            return new SessionLogSelection(
                    logFilesResult.logFiles().stream()
                            .sorted(Comparator.comparingInt(LogFile::getThread)
                                    .thenComparing(LogFile::getSequence))
                            .toList(),
                    upperBoundary);
        }

        LOGGER.debug("Using capped logs, reading up to {}.", effectiveUpperBoundary);
        // Use the calculated effective upper boundary
        // Resort the capped log files in thread+sequence order for application
        previousEffectiveUpperBoundary = effectiveUpperBoundary;
        return new SessionLogSelection(
                cappedLogsByThread.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .flatMap(entry -> entry.getValue().stream())
                        .toList(),
                effectiveUpperBoundary);
    }

    private Map<Integer, List<LogFile>> getThreadLogsCappedBySize(Map<Integer, List<LogFile>> logsByThread, long thresholdBytes) {
        final Map<Integer, List<LogFile>> logsByThreadCapped = new HashMap<>();
        for (Map.Entry<Integer, List<LogFile>> entry : logsByThread.entrySet()) {
            final List<LogFile> threadLogs = entry.getValue();
            final List<LogFile> cappedLogs = new ArrayList<>();

            long accumulatedSize = 0;
            int nextIndex = 0;
            while (nextIndex < threadLogs.size()) {
                final LogFile logFile = threadLogs.get(nextIndex);
                accumulatedSize += logFile.getBytes();
                cappedLogs.add(logFile);
                nextIndex++;

                if (accumulatedSize >= thresholdBytes) {
                    break;
                }
            }

            // The effective upper boundary must never regress below a boundary a previous session
            // already mined; such a session re-reads redo without producing any new events. Extend
            // the capped window until its top passes the previously mined boundary.
            if (previousEffectiveUpperBoundary != null) {
                while (nextIndex < threadLogs.size() && isWindowTopAtOrBelow(cappedLogs, previousEffectiveUpperBoundary)) {
                    cappedLogs.add(threadLogs.get(nextIndex));
                    nextIndex++;
                }
            }

            // When the capped window has consumed all the thread's archive logs and reached the
            // online redo logs, include the thread's remaining online logs so the session reads
            // through the online upper boundary rather than being capped at a non-current online
            // log; the marginal cost is bounded by the thread's redo log group count.
            if (!cappedLogs.get(cappedLogs.size() - 1).isArchive()) {
                while (nextIndex < threadLogs.size()) {
                    cappedLogs.add(threadLogs.get(nextIndex));
                    nextIndex++;
                }
            }

            logsByThreadCapped.put(entry.getKey(), cappedLogs);
        }
        return logsByThreadCapped;
    }

    private static boolean isWindowTopAtOrBelow(List<LogFile> windowLogs, Scn boundary) {
        final LogFile lastWindowLog = windowLogs.get(windowLogs.size() - 1);
        return !lastWindowLog.isCurrent() && lastWindowLog.getNextScn().compareTo(boundary) <= 0;
    }
}
