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
    private Map<Integer, List<LogFile>> previousBudgetLogsByThread;
    private Scn previousEffectiveUpperBoundary;
    private boolean deriveLogCountFromSeed;

    /**
     * Creates a capped log file session selector.
     *
     * The previously mined boundary seeds the capped window state from the restored offsets so the
     * window guarantees survive a connector restart. The seed is a lower bound on the upper boundary
     * of the last mining session before the restart; the log count per redo thread is re-derived from
     * the seeded span on the first selection, when the collected logs provide the byte sizes needed
     * to translate the span into a per-thread log count.
     *
     * @param minimumLogsPerRedoThread minimum number of logs to mine per redo thread
     * @param redoLogSizeInBytes maximum size of an online redo log in bytes
     * @param previouslyMinedBoundary lower bound on the previously mined upper boundary; ignored when null or none
     */
    public CappedLogFileSessionSelector(int minimumLogsPerRedoThread, long redoLogSizeInBytes, Scn previouslyMinedBoundary) {
        this.minimumLogsPerRedoThread = minimumLogsPerRedoThread;
        this.redoLogSizeInBytes = redoLogSizeInBytes;
        this.logsPerRedoThread = minimumLogsPerRedoThread;
        if (previouslyMinedBoundary != null && !previouslyMinedBoundary.isNull()) {
            this.previousEffectiveUpperBoundary = previouslyMinedBoundary;
            this.deriveLogCountFromSeed = true;
        }
    }

    @Override
    public SessionLogSelection selectLogsForSession(LogFilesResult logFilesResult, Scn upperBoundary) {
        Scn effectiveUpperBoundary = upperBoundary;

        // Groups all collected logs by redo thread, sorted in ascending order by sequence.
        // The ordering is important for this algorithm when inspecting what is the first/last logs per thread.
        final Map<Integer, List<LogFile>> logsByThread = logFilesResult.logFiles().stream()
                .sorted(Comparator.comparing(LogFile::getSequence))
                .collect(Collectors.groupingBy(LogFile::getThread));

        if (deriveLogCountFromSeed) {
            deriveLogCountFromSeed = false;
            logsPerRedoThread = deriveLogsPerRedoThread(logsByThread);
        }

        Map<Integer, List<LogFile>> budgetLogsByThread = getThreadLogsCappedByBudget(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);

        if (previousBudgetLogsByThread != null && budgetLogsByThread.equals(previousBudgetLogsByThread)) {
            logsPerRedoThread++;
            LOGGER.debug("Capped log set unchanged, growing log count per redo thread to {}.", logsPerRedoThread);
            budgetLogsByThread = getThreadLogsCappedByBudget(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);
        }

        previousBudgetLogsByThread = budgetLogsByThread;

        Map<Integer, List<LogFile>> cappedLogsByThread = extendPastPreviousBoundary(logsByThread, budgetLogsByThread);

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
            if (logsPerRedoThread > minimumLogsPerRedoThread) {
                logsPerRedoThread = minimumLogsPerRedoThread;
                LOGGER.debug("All threads reading online redo, resetting log count per redo thread to {}.", logsPerRedoThread);
            }
            // Growth only widens a window capped below the online redo logs; after an online pass
            // there is no cap to widen, so clear the baseline to avoid growing the log count on
            // the next iteration only to reset it within the same call.
            previousBudgetLogsByThread = null;
            recordEffectiveUpperBoundary(upperBoundary);
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
        recordEffectiveUpperBoundary(effectiveUpperBoundary);
        return new SessionLogSelection(
                cappedLogsByThread.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .flatMap(entry -> entry.getValue().stream())
                        .toList(),
                effectiveUpperBoundary);
    }

    private int deriveLogsPerRedoThread(Map<Integer, List<LogFile>> logsByThread) {
        // The seeded boundary marks ground already mined before the restart; the per-thread byte
        // span up to it re-expresses the window width the budget had grown to, so the first
        // session resumes at that width rather than re-climbing from the minimum.
        long maxThreadBytes = 0;
        for (List<LogFile> threadLogs : logsByThread.values()) {
            long threadBytes = 0;
            for (LogFile logFile : threadLogs) {
                if (logFile.getFirstScn().compareTo(previousEffectiveUpperBoundary) >= 0) {
                    break;
                }
                threadBytes += logFile.getBytes();
            }
            maxThreadBytes = Math.max(maxThreadBytes, threadBytes);
        }

        final int derived = Math.toIntExact(Math.max(minimumLogsPerRedoThread, (maxThreadBytes + redoLogSizeInBytes - 1) / redoLogSizeInBytes));
        LOGGER.debug("Derived log count per redo thread {} from previously mined boundary {}.", derived, previousEffectiveUpperBoundary);

        return derived;
    }

    private Map<Integer, List<LogFile>> getThreadLogsCappedByBudget(Map<Integer, List<LogFile>> logsByThread, long thresholdBytes) {
        final Map<Integer, List<LogFile>> logsByThreadCapped = new HashMap<>();
        for (Map.Entry<Integer, List<LogFile>> entry : logsByThread.entrySet()) {
            final List<LogFile> cappedLogs = new ArrayList<>();

            long accumulatedSize = 0;
            for (LogFile logFile : entry.getValue()) {
                accumulatedSize += logFile.getBytes();
                cappedLogs.add(logFile);

                if (accumulatedSize >= thresholdBytes) {
                    break;
                }
            }

            logsByThreadCapped.put(entry.getKey(), cappedLogs);
        }
        return logsByThreadCapped;
    }

    private Map<Integer, List<LogFile>> extendPastPreviousBoundary(Map<Integer, List<LogFile>> logsByThread,
                                                                   Map<Integer, List<LogFile>> budgetCapped) {
        final Map<Integer, List<LogFile>> result = new HashMap<>();
        for (Map.Entry<Integer, List<LogFile>> entry : logsByThread.entrySet()) {
            final List<LogFile> threadLogs = entry.getValue();
            final List<LogFile> budgetLogs = budgetCapped.get(entry.getKey());
            final List<LogFile> extended = new ArrayList<>(budgetLogs);
            int nextIndex = budgetLogs.size();

            if (previousEffectiveUpperBoundary != null) {
                while (nextIndex < threadLogs.size() && isWindowTopAtOrBelow(extended, previousEffectiveUpperBoundary)) {
                    extended.add(threadLogs.get(nextIndex));
                    nextIndex++;
                }
                if (nextIndex > budgetLogs.size()) {
                    LOGGER.debug("Extended thread {} window by {} logs past previously mined boundary {}.",
                            entry.getKey(), nextIndex - budgetLogs.size(), previousEffectiveUpperBoundary);
                }
            }

            if (!extended.get(extended.size() - 1).isArchive()) {
                while (nextIndex < threadLogs.size()) {
                    extended.add(threadLogs.get(nextIndex));
                    nextIndex++;
                }
            }

            result.put(entry.getKey(), extended);
        }
        return result;
    }

    private static boolean isWindowTopAtOrBelow(List<LogFile> windowLogs, Scn boundary) {
        final LogFile lastWindowLog = windowLogs.get(windowLogs.size() - 1);
        return !lastWindowLog.isCurrent() && lastWindowLog.getNextScn().compareTo(boundary) <= 0;
    }

    private void recordEffectiveUpperBoundary(Scn effectiveUpperBoundary) {
        // Track the highest boundary handed to a mining session; used as the floor that
        // subsequent capped windows must be extended past.
        if (previousEffectiveUpperBoundary == null || effectiveUpperBoundary.compareTo(previousEffectiveUpperBoundary) > 0) {
            previousEffectiveUpperBoundary = effectiveUpperBoundary;
        }
    }
}
