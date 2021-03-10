/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.Objects;

/**
 * Represents a redo or archive log in Oracle.
 *
 * @author Chris Cranford
 */
public class LogFile {

    private static final String CURRENT = "CURRENT";

    private final String fileName;
    private final Scn firstScn;
    private final Scn nextScn;
    private final String status;

    /**
     * Create a log file that represents an archived log record.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     */
    public LogFile(String fileName, Scn firstScn, Scn nextScn) {
        this(fileName, firstScn, nextScn, null);
    }

    /**
     * Creates a log file that represents an online redo log record.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param status the status
     */
    public LogFile(String fileName, Scn firstScn, Scn nextScn, String status) {
        this.fileName = fileName;
        this.firstScn = firstScn;
        this.nextScn = nextScn;
        this.status = status;
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

    /**
     * Returns whether this log file instance is considered the current online redo log record.
     */
    public boolean isCurrent() {
        return CURRENT.equalsIgnoreCase(status);
    }

    /**
     * Returns whether the specified {@code other} log file has the same SCN range as this instance.
     *
     * @param other the other log file instance
     * @return true if both have the same SCN range; otherwise false
     */
    public boolean isSameRange(LogFile other) {
        return Objects.equals(firstScn, other.getFirstScn()) && Objects.equals(nextScn, other.getNextScn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstScn, nextScn);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LogFile)) {
            return false;
        }
        return isSameRange((LogFile) obj);
    }
}
