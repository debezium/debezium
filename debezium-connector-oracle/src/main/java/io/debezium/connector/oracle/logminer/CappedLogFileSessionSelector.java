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
    private final int maximumLogsPerRedoThread;

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
     * @param maximumLogsPerRedoThread growth ceiling for the log count per redo thread; a minimum above it takes precedence
     * @param previouslyMinedBoundary lower bound on the previously mined upper boundary; ignored when null or none
     */
    public CappedLogFileSessionSelector(int minimumLogsPerRedoThread, int maximumLogsPerRedoThread, Scn previouslyMinedBoundary) {
        this.minimumLogsPerRedoThread = minimumLogsPerRedoThread;
        this.maximumLogsPerRedoThread = maximumLogsPerRedoThread;
        this.logsPerRedoThread = minimumLogsPerRedoThread;
        if (previouslyMinedBoundary != null && !previouslyMinedBoundary.isNull()) {
            this.previousEffectiveUpperBoundary = previouslyMinedBoundary;
            this.deriveLogCountFromSeed = true;
        }
    }

    @Override
    public SessionLogSelection selectLogsForSession(LogFilesResult logFilesResult, Scn upperBoundary) {
        // Groups all collected logs by redo thread, sorted in ascending order by sequence.
        // The ordering is important for this algorithm when inspecting what is the first/last logs per thread.
        final Map<Integer, List<LogFile>> logsByThread = logFilesResult.logFiles().stream()
                .sorted(Comparator.comparing(LogFile::getSequence))
                .collect(Collectors.groupingBy(LogFile::getThread));

        // Resolved once per selection so the budget and the count derived back out of it share the
        // same unit; a value that drifted between the two would skew the round trip.
        final long redoLogSizeInBytes = resolveRedoLogSizeInBytes(logsByThread);

        if (deriveLogCountFromSeed) {
            deriveLogCountFromSeed = false;
            // The seeded boundary alone preserves the pre-restart window via the extension, so
            // clamping the derived count costs no coverage; it bounds the slice width carried into
            // catch-up when the pin clears before any stall can apply the growth ceiling.
            logsPerRedoThread = clampToGrowthCeiling(deriveLogsPerRedoThread(logsByThread, redoLogSizeInBytes));
        }

        Map<Integer, List<LogFile>> budgetLogsByThread = getThreadLogsCappedByBudget(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);

        if (previousBudgetLogsByThread != null && budgetLogsByThread.equals(previousBudgetLogsByThread)) {
            // Derive the window width from the stall distance so the budget covers the already mined
            // ground in one step instead of one log per session. The +1 floor preserves the previous
            // linear-growth guarantee.
            final int derivedLogsPerRedoThread = deriveLogsPerRedoThread(logsByThread, redoLogSizeInBytes);
            logsPerRedoThread = clampToGrowthCeiling(Math.max(derivedLogsPerRedoThread, logsPerRedoThread + 1));
            LOGGER.debug("Capped log set unchanged, growing log count per redo thread to {}.", logsPerRedoThread);
            budgetLogsByThread = getThreadLogsCappedByBudget(logsByThread, (long) logsPerRedoThread * redoLogSizeInBytes);
        }

        previousBudgetLogsByThread = budgetLogsByThread;

        Map<Integer, List<LogFile>> cappedLogsByThread = extendPastPreviousBoundary(logsByThread, budgetLogsByThread);

        final WindowInspection inspection = inspectWindow(logFilesResult, cappedLogsByThread, logsByThread, upperBoundary);

        if (inspection.allThreadsMineOnline()) {
            LOGGER.debug("All threads are reading online redo, using all logs and reading up to {}.", upperBoundary);
            resetWindowGrowth("All threads reading online redo");
            recordEffectiveUpperBoundary(upperBoundary);
            return new SessionLogSelection(
                    logFilesResult.logFiles().stream()
                            .sorted(Comparator.comparingInt(LogFile::getThread)
                                    .thenComparing(LogFile::getSequence))
                            .toList(),
                    upperBoundary);
        }

        if (inspection.atEndOfAvailableLogs()) {
            // The window covers everything on offer, so the growth accrued while catching up has
            // nothing left to widen. The boundary still comes from the capped path below, as the
            // logs stop short of the unbounded upper boundary.
            resetWindowGrowth("All collected logs are within the window");
        }

        LOGGER.debug("Using capped logs, reading up to {}.", inspection.effectiveUpperBoundary());
        // Use the calculated effective upper boundary
        // Resort the capped log files in thread+sequence order for application
        recordEffectiveUpperBoundary(inspection.effectiveUpperBoundary());
        return new SessionLogSelection(
                cappedLogsByThread.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .flatMap(entry -> entry.getValue().stream())
                        .toList(),
                inspection.effectiveUpperBoundary());
    }

    /**
     * The outcome of measuring the selected window against the logs that were collected.
     *
     * @param allThreadsMineOnline every open redo thread's window ends on the current online redo log,
     *            so the unbounded upper boundary is covered and every collected log can be mined
     * @param effectiveUpperBoundary the boundary to mine up to, tightened to the smallest next SCN
     *            across the threads whose window ends on an archive
     * @param atEndOfAvailableLogs the window already holds every collected log, so the budget is no
     *            longer the binding constraint
     */
    private record WindowInspection(boolean allThreadsMineOnline, Scn effectiveUpperBoundary, boolean atEndOfAvailableLogs) {
    }

    /**
     * Measures the selected window against the collected logs.
     *
     * <p>Two independent signals come out of this. A window ending on the current online redo log for
     * every open redo thread covers the unbounded upper boundary, so the session may mine every log up
     * to it. Separately, a window already holding every collected log means the budget stopped being
     * the binding constraint. A stream that never collects an online redo log, such as a physical
     * standby or archive-only mining, can never raise the first signal, leaving the second as its only
     * indication that there is nothing further to mine. The two are kept apart because only the first
     * makes the unbounded upper boundary safe to record.
     *
     * @param logFilesResult the collected logs and the redo thread state they were collected against
     * @param cappedLogsByThread the window selected for this session, grouped by redo thread
     * @param logsByThread every collected log, grouped by redo thread
     * @param upperBoundary the boundary to tighten from
     * @return the measurements taken of the window
     */
    private WindowInspection inspectWindow(LogFilesResult logFilesResult,
                                           Map<Integer, List<LogFile>> cappedLogsByThread,
                                           Map<Integer, List<LogFile>> logsByThread,
                                           Scn upperBoundary) {
        Scn effectiveUpperBoundary = upperBoundary;
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

        // Restricted to streams that collected no online redo log so that mining from a primary keeps
        // resetting solely off the branch above. A larger budget cannot select more than the window
        // already holds, so raising this signal can never narrow the window it was raised for.
        final boolean atEndOfAvailableLogs = !allThreadsMineOnline
                && cappedLogsByThread.equals(logsByThread)
                && logsByThread.values().stream().flatMap(List::stream).allMatch(LogFile::isArchive);

        return new WindowInspection(allThreadsMineOnline, effectiveUpperBoundary, atEndOfAvailableLogs);
    }

    /**
     * Returns the window to its configured width once growth has nothing left to widen.
     *
     * @param reason why the growth is being reset, for the debug log
     */
    private void resetWindowGrowth(String reason) {
        if (logsPerRedoThread > minimumLogsPerRedoThread) {
            logsPerRedoThread = minimumLogsPerRedoThread;
            LOGGER.debug("{}, resetting log count per redo thread to {}.", reason, logsPerRedoThread);
        }
        // Growth only widens a window capped below the logs on offer; once the window holds them all
        // there is no cap to widen, so clear the baseline to avoid growing the log count on the next
        // iteration only to reset it within the same call.
        previousBudgetLogsByThread = null;
    }

    /**
     * Clamps a candidate log count per redo thread to the growth ceiling.
     *
     * The ceiling bounds how wide the budget may grow, whether from stall growth or from the count
     * derived off a seeded boundary on restart, keeping the budget's contribution to a single
     * mining session predictable. A configured minimum above the ceiling wins, so the count never
     * drops below the user's configuration. The mined window itself may still exceed the ceiling
     * when extending past the previously mined boundary, as that ground must be re-covered
     * regardless of the budget.
     *
     * @param logCount the candidate log count per redo thread
     * @return the log count, never greater than the growth ceiling
     */
    private int clampToGrowthCeiling(int logCount) {
        return Math.min(logCount, Math.max(maximumLogsPerRedoThread, minimumLogsPerRedoThread));
    }

    /**
     * Resolves the size of a full redo log, in bytes, from the collected logs.
     *
     * The budget is carried in bytes rather than in a log count so that a burst of forced log
     * switches, which archives logs far smaller than a full redo log, is swept into a single mining
     * session instead of crawling one small log at a time. The unit that converts the configured log
     * count into that byte budget is the largest collected log, because an archive is a copy of a
     * filled redo log and a forced switch only ever makes it smaller, never larger. Reading the unit
     * back off the logs keeps it correct wherever the session mines, including a physical standby
     * whose local online redo logs may be sized differently from the primary's.
     *
     * @param logsByThread the collected logs grouped by redo thread
     * @return the largest collected log size in bytes, or {@code 0} when no sizes are available
     */
    private static long resolveRedoLogSizeInBytes(Map<Integer, List<LogFile>> logsByThread) {
        return logsByThread.values().stream()
                .flatMap(List::stream)
                .mapToLong(LogFile::getBytes)
                .max()
                .orElse(0L);
    }

    private int deriveLogsPerRedoThread(Map<Integer, List<LogFile>> logsByThread, long redoLogSizeInBytes) {
        if (redoLogSizeInBytes <= 0) {
            // Without a usable unit there is no byte span to translate, so the configured minimum is
            // the only width the selection can promise.
            return minimumLogsPerRedoThread;
        }

        // The previously mined boundary marks ground already covered; the per-thread byte span up
        // to it re-expresses the window width required to cover that ground, so a seeded restart
        // or a stalled budget resumes at that width rather than re-climbing from the minimum.
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

                // The log count is the user-facing contract, so the byte budget never closes the
                // window below it. The unit resolved from the collected logs already implies this,
                // as no single log can exceed it, but stating it here keeps the guarantee from
                // resting on that and holds when no log reports a size.
                if (accumulatedSize >= thresholdBytes && cappedLogs.size() >= minimumLogsPerRedoThread) {
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
