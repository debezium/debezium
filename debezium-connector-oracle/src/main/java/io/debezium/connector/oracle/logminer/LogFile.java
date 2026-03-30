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
    private final long bytes;
    private final boolean dictionaryStart;
    private final boolean dictionaryEnd;

    /**
     * Creates an archive log file.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param thread the redo thread the log belongs
     * @param bytes the size of the log in bytes
     * @param dictionaryStart whether the dictionary start marker is present
     * @param dictionaryEnd whether the dictionary end marker is present
     * @return a log file record for an archive log row
     */
    public static LogFile forArchive(String fileName, Scn firstScn, Scn nextScn, BigInteger sequence, int thread, long bytes, boolean dictionaryStart,
                                     boolean dictionaryEnd) {
        return new LogFile(fileName, firstScn, nextScn, sequence, Type.ARCHIVE, false, thread, bytes, dictionaryStart, dictionaryEnd);
    }

    /**
     * Creates an online redo log file.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param current whether the online redo log is marked as the current one being written
     * @param thread the redo thread the log belongs
     * @param bytes the size of the log in bytes
     * @return a log file record for an online redo log row
     */
    public static LogFile forRedo(String fileName, Scn firstScn, Scn nextScn, BigInteger sequence, boolean current, int thread, long bytes) {
        return new LogFile(fileName, firstScn, nextScn, sequence, Type.REDO, current, thread, bytes, false, false);
    }

    /**
     * Creates a log file.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param type the type of archive log
     * @param current whether the log file is the current one
     * @param thread the redo thread the log is assigned
     * @param bytes the size of the log in bytes
     * @param dictionaryStart whether the dictionary start marker is present
     * @param dictionaryEnd whether the dictionary end marker is present
     */
    private LogFile(String fileName, Scn firstScn, Scn nextScn, BigInteger sequence, Type type, boolean current, int thread,
                    long bytes, boolean dictionaryStart, boolean dictionaryEnd) {
        this.fileName = fileName;
        this.firstScn = firstScn;
        this.nextScn = nextScn;
        this.sequence = sequence;
        this.current = current;
        this.type = type;
        this.thread = thread;
        this.bytes = bytes;
        this.dictionaryStart = dictionaryStart;
        this.dictionaryEnd = dictionaryEnd;
    }

    public String getFileName() {
        return fileName;
    }

    public Scn getFirstScn() {
        return firstScn;
    }

    public Scn getNextScn() {
        return nextScn;
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
        return getFirstScn().compareTo(scn) <= 0 && getNextScn().compareTo(scn) > 0;
    }

    public boolean isArchive() {
        return type == Type.ARCHIVE;
    }

    public boolean isRedo() {
        return type == Type.REDO;
    }

    public long getBytes() {
        return bytes;
    }

    public boolean hasDictionaryStart() {
        return dictionaryStart;
    }

    public boolean hasDictionaryEnd() {
        return dictionaryEnd;
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
                ", bytes=" + bytes +
                ", dictStart=" + dictionaryStart +
                ", dictEnd=" + dictionaryEnd +
                '}';
    }
}
