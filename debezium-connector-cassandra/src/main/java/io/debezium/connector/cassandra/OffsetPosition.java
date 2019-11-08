/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorDataException;

/**
 * The OffsetPosition uniquely identifies a specific {@link org.apache.cassandra.db.Mutation} in a specific commit log.
 */
public class OffsetPosition implements Comparable<OffsetPosition> {
    public static final String DEFAULT_FILENAME = "";
    public static final int DEFAULT_POSITION = -1;

    public final String fileName;
    public final int filePosition;

    public OffsetPosition(String fileName, int filePosition) {
        this.fileName = fileName;
        this.filePosition = filePosition;
    }

    public String serialize() {
        return fileName + File.pathSeparatorChar + filePosition;
    }

    public static OffsetPosition parse(String offset) {
        String[] fileAndPos = offset.split(Character.toString(File.pathSeparatorChar));
        if (fileAndPos.length != 2) {
            throw new CassandraConnectorDataException("OffsetPosition should have a file and a position, but got " + Arrays.toString(fileAndPos));
        }
        return new OffsetPosition(fileAndPos[0], Integer.parseInt(fileAndPos[1]));
    }

    public static OffsetPosition defaultOffsetPosition() {
        return new OffsetPosition(DEFAULT_FILENAME, DEFAULT_POSITION);
    }

    @Override
    public int compareTo(OffsetPosition other) {
        if (this == other) {
            return 0;
        }
        int result = CommitLogUtil.compareCommitLogs(this.fileName, other.fileName);
        if (result == 0) {
            result = Integer.compare(filePosition, other.filePosition);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetPosition that = (OffsetPosition) o;
        return filePosition == that.filePosition && Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, filePosition);
    }

    @Override
    public String toString() {
        return serialize();
    }
}
