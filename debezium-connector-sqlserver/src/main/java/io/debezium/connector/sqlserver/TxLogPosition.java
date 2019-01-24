/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

/**
 * Defines a position of change in the transaction log. The position is defined as a combination of commit LSN
 * and sequence number of the change in the given transaction.
 * The sequence number is monotonically increasing in transaction but it is not guaranteed across multiple
 * transactions so the combination is necessary to get total order.
 *
 * @author Jiri Pechanec
 *
 */
public class TxLogPosition implements Nullable, Comparable<TxLogPosition> {
    public static final TxLogPosition NULL = new TxLogPosition(null, null);
    private final Lsn commitLsn;
    private final Lsn inTxLsn;

    private TxLogPosition(Lsn commitLsn, Lsn inTxLsn) {
        this.commitLsn = commitLsn;
        this.inTxLsn = inTxLsn;
    }

    public Lsn getCommitLsn() {
        return commitLsn;
    }

    public Lsn getInTxLsn() {
        return inTxLsn;
    }

    @Override
    public String toString() {
        return this == NULL ? "NULL" : commitLsn + "(" + inTxLsn + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((commitLsn == null) ? 0 : commitLsn.hashCode());
        result = prime * result + ((inTxLsn == null) ? 0 : inTxLsn.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TxLogPosition other = (TxLogPosition) obj;
        if (commitLsn == null) {
            if (other.commitLsn != null)
                return false;
        }
        else if (!commitLsn.equals(other.commitLsn))
            return false;
        if (inTxLsn == null) {
            if (other.inTxLsn != null)
                return false;
        }
        else if (!inTxLsn.equals(other.inTxLsn))
            return false;
        return true;
    }

    @Override
    public int compareTo(TxLogPosition o) {
        final int comparison = commitLsn.compareTo(o.getCommitLsn());
        return comparison == 0 ? inTxLsn.compareTo(inTxLsn) : comparison;
    }

    public static TxLogPosition valueOf(Lsn commitLsn, Lsn inTxLsn) {
        return commitLsn == null && inTxLsn == null ? NULL
                : new TxLogPosition(
                        commitLsn == null ? Lsn.NULL : commitLsn,
                        inTxLsn == null ? Lsn.NULL : inTxLsn
        );
    }

    public static TxLogPosition valueOf(Lsn commitLsn) {
        return valueOf(commitLsn, Lsn.NULL);
    }

    @Override
    public boolean isAvailable() {
        return inTxLsn != null && commitLsn != null;
    }
}