/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import java.util.List;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;

/**
 * Decides when the unbuffered LogMiner adapter recalculates its resume position.
 * <p>
 * Recalculating the resume position requires a secondary LogMiner session, see {@link ResumePositionCalculator},
 * so a strategy balances how far the resume position may lag behind against the cost of that session.
 *
 * @author Chris Cranford
 */
public interface ResumePositionUpdateStrategy {

    /**
     * Records that a transaction was committed.
     *
     * @param eventCount the number of events in the committed transaction
     */
    void onCommit(int eventCount);

    /**
     * Returns whether the resume position should be recalculated now.
     *
     * @param resumeScn the current resume system change number, should not be {@code null}
     * @param commitScn the current commit system change number, should not be {@code null}
     * @param logFiles the logs that the resume position would be calculated from, should not be {@code null}
     * @return {@code true} if the resume position should be recalculated, {@code false} otherwise
     */
    boolean shouldUpdate(Scn resumeScn, Scn commitScn, List<LogFile> logFiles);

    /**
     * Records that the resume position was recalculated, whether successfully or not.
     *
     * @param commitScn the commit system change number the recalculation was based on, should not be {@code null}
     */
    void onUpdated(Scn commitScn);
}
