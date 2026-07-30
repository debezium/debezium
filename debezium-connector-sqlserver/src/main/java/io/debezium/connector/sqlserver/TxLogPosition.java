/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.connector.Nullable;

/**
 * Defines a position of change in the transaction log. The position is defined as a combination of commit LSN
 * and sequence number of the change in the given transaction.
 * The sequence number is monotonically increasing in transaction but it is not guaranteed across multiple
 * transactions so the combination is necessary to get total order.
 * <p>
 * The command id is only available when the change table is read directly
 * ({@code data.query.mode=direct}). In function mode it is therefore always {@code null}
 *
 * @author Jiri Pechanec
 *
 */
public class TxLogPosition implements Nullable, Comparable<TxLogPosition> {

    public static final TxLogPosition NULL = new TxLogPosition(null, null, 0, null);
    private final Lsn commitLsn;
    private final Lsn inTxLsn;
    private int operation;
    private final Integer commandId;

    private TxLogPosition(Lsn commitLsn, Lsn inTxLsn, int operation, Integer commandId) {
        this.commitLsn = commitLsn;
        this.inTxLsn = inTxLsn;
        this.operation = operation;
        this.commandId = commandId;
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    public Lsn getInTxLsn() {
        return inTxLsn;
    }

    public int getOperation() {
        return operation;
    }

    public Integer getCommandId() {
        return commandId;
    }

    @Override
    public String toString() {
        return this == NULL ? "NULL" : commitLsn + "(" + inTxLsn + "," + operation + "," + commandId + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((commitLsn == null) ? 0 : commitLsn.hashCode());
        result = prime * result + ((commandId == null) ? 0 : commandId.hashCode());
        result = prime * result + ((inTxLsn == null) ? 0 : inTxLsn.hashCode());
        result = prime * result + operation;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TxLogPosition other = (TxLogPosition) obj;
        if (commitLsn == null) {
            if (other.commitLsn != null) {
                return false;
            }
        }
        else if (!commitLsn.equals(other.commitLsn)) {
            return false;
        }

        if (commandId == null) {
            if (other.commandId != null) {
                return false;
            }
        }
        else if (!commandId.equals(other.commandId)) {
            return false;
        }

        if (inTxLsn == null) {
            if (other.inTxLsn != null) {
                return false;
            }
        }
        else if (!inTxLsn.equals(other.inTxLsn)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(TxLogPosition o) {
        int comparison = commitLsn.compareTo(o.getCommitLsn());
        if (comparison != 0) {
            return comparison;
        }

        if (commandId != null && o.getCommandId() != null) {
            comparison = commandId.compareTo(o.getCommandId());
            if (comparison != 0) {
                return comparison;
            }
        }
        return inTxLsn.compareTo(o.inTxLsn);
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn inTxLsn, int operation, Integer commandId) {
        return commitLsn == null && inTxLsn == null && commandId == null ? NULL
                : new TxLogPosition(
                        commitLsn == null ? Lsn.NULL : commitLsn,
                        inTxLsn == null ? Lsn.NULL : inTxLsn,
                        operation,
                        commandId);
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn inTxLsn, int operation) {
        return valueOf(commitLsn, inTxLsn, operation, null);
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn inTxLsn) {
        return valueOf(commitLsn, inTxLsn, 0, null);
    }

    public static TxLogPosition valueOf(Lsn commitLsn) {
        return valueOf(commitLsn, Lsn.NULL, 0, null);
    }

    @Override
    public boolean isAvailable() {
        return inTxLsn != null && commitLsn != null;
    }
}
