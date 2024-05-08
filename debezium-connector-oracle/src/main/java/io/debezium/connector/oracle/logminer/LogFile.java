/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.util.Objects;

import io.debezium.connector.oracle.Scn;

/**
 * Represents a redo or archive log in Oracle.
 *
 * @author Chris Cranford
 */
public class LogFile {

    public enum Type {
        ARCHIVE,
        REDO
    }

    private final String fileName;
    private final Scn firstScn;
    private final Scn nextScn;
    private final BigInteger sequence;
    private final boolean current;
    private final Type type;
    private final int thread;

    /**
     * Create a log file that represents an archived log record.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param type the log type
     */
    public LogFile(String fileName, Scn firstScn, Scn nextScn, BigInteger sequence, Type type, int thread) {
        this(fileName, firstScn, nextScn, sequence, type, false, thread);
    }

    /**
     * Creates a log file that represents an online redo log record.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param type the type of archive log
     * @param current whether the log file is the current one
     */
    public LogFile(String fileName, Scn firstScn, Scn nextScn, BigInteger sequence, Type type, boolean current, int thread) {
        this.fileName = fileName;
        this.firstScn = firstScn;
        this.nextScn = nextScn;
        this.sequence = sequence;
        this.current = current;
        this.type = type;
        this.thread = thread;
    }

    public String getFileName() {
        return fileName;
    }

    public Scn getFirstScn() {
        return firstScn;
    }

    public Scn getNextScn() {
        return isCurrent() ? Scn.MAX : nextScn;
    }

    public BigInteger getSequence() {
        return sequence;
    }

    public int getThread() {
        return thread;
    }

    /**
     * Returns whether this log file instance is considered the current online redo log record.
     */
    public boolean isCurrent() {
        return current;
    }

    public Type getType() {
        return type;
    }

    public boolean isScnInLogFileRange(Scn scn) {
        return getFirstScn().compareTo(scn) <= 0 && (getNextScn().compareTo(scn) > 0 || getNextScn().equals(Scn.MAX));
    }

    @Override
    public int hashCode() {
        return Objects.hash(thread, sequence);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LogFile)) {
            return false;
        }
        final LogFile other = (LogFile) obj;
        return thread == other.thread && Objects.equals(sequence, other.sequence);
    }

    @Override
    public String toString() {
        return "LogFile{" +
                "fileName='" + fileName + '\'' +
                ", firstScn=" + firstScn +
                ", nextScn=" + nextScn +
                ", sequence=" + sequence +
                ", current=" + current +
                ", type=" + type +
                ", thread=" + thread +
                '}';
    }
}
