/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.nio.ByteBuffer;

import org.postgresql.replication.LogSequenceNumber;

/**
 * Abstraction of PostgreSQL log sequence number, adapted from
 * {@link org.postgresql.replication.LogSequenceNumber}.
 *
 * @author Jiri Pechanec
 *
 */
public class Lsn implements Comparable<Lsn> {

    /**
     * Zero is used indicate an invalid pointer. Bootstrap skips the first
     * possible WAL segment, initializing the first WAL page at XLOG_SEG_SIZE,
     * so no XLOG record can begin at zero.
     */
    public static final Lsn INVALID_LSN = new Lsn(0);

    private final long value;

    private Lsn(long value) {
        this.value = value;
    }

    /**
     * @param value
     *            numeric represent position in the write-ahead log stream
     * @return not null LSN instance
     */
    public static Lsn valueOf(Long value) {
        if (value == null) {
            return null;
        }
        if (value == 0) {
            return INVALID_LSN;
        }
        return new Lsn(value);
    }

    /**
     * @param value
     *            PostgreSQL JDBC driver domain type representing position in the write-ahead log stream
     * @return not null LSN instance
     */
    public static Lsn valueOf(LogSequenceNumber value) {
        if (value.asLong() == 0) {
            return INVALID_LSN;
        }
        return new Lsn(value.asLong());
    }

    /**
     * Create LSN instance by string represent LSN.
     *
     * @param strValue
     *            not null string as two hexadecimal numbers of up to 8 digits
     *            each, separated by a slash. For example {@code 16/3002D50},
     *            {@code 0/15D68C50}
     * @return not null LSN instance where if specified string represent have
     *         not valid form {@link Lsn#INVALID_LSN}
     */
    public static Lsn valueOf(String strValue) {
        final int slashIndex = strValue.lastIndexOf('/');

        if (slashIndex <= 0) {
            return INVALID_LSN;
        }

        final String logicalXLogStr = strValue.substring(0, slashIndex);
        final int logicalXlog = (int) Long.parseLong(logicalXLogStr, 16);
        final String segmentStr = strValue.substring(slashIndex + 1, strValue.length());
        final int segment = (int) Long.parseLong(segmentStr, 16);

        final ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(logicalXlog);
        buf.putInt(segment);
        buf.position(0);
        final long value = buf.getLong();

        return Lsn.valueOf(value);
    }

    /**
     * @return Long represent position in the write-ahead log stream
     */
    public long asLong() {
        return value;
    }

    /**
     * @return PostgreSQL JDBC driver representation of position in the write-ahead log stream
     */
    public LogSequenceNumber asLogSequenceNumber() {
        return LogSequenceNumber.valueOf(value);
    }

    /**
     * @return String represent position in the write-ahead log stream as two
     *         hexadecimal numbers of up to 8 digits each, separated by a slash.
     *         For example {@code 16/3002D50}, {@code 0/15D68C50}
     */
    public String asString() {
        final ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value);
        buf.position(0);

        final int logicalXlog = buf.getInt();
        final int segment = buf.getInt();
        return String.format("%X/%X", logicalXlog, segment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Lsn that = (Lsn) o;

        return value == that.value;

    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    public boolean isValid() {
        return this != INVALID_LSN;
    }

    @Override
    public String toString() {
        return "LSN{" + asString() + '}';
    }

    @Override
    public int compareTo(Lsn o) {
        if (value == o.value) {
            return 0;
        }
        // Unsigned comparison
        return value + Long.MIN_VALUE < o.value + Long.MIN_VALUE ? -1 : 1;
    }
}
