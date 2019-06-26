/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

/**
 * Interface for recording offset.
 */
public interface OffsetWriter {

    /**
     * Update the offset in memory if the provided offset is greater than the existing offset.
     * @param sourceTable string in the format of <keyspace>.<table>.
     * @param sourceOffset string in the format of <file_name>:<file_position>
     * @param isSnapshot whether the offset is coming from a snapshot or commit log
     */
    void markOffset(String sourceTable, String sourceOffset, boolean isSnapshot);

    /**
     * Determine if an offset has been processed based on the table name, offset position, and whether
     * it is from snapshot or not.
     * @param sourceTable string in the format of <keyspace>.<table>.
     * @param sourceOffset string in the format of <file_name>:<file_position>
     * @param isSnapshot whether the offset is coming from a snapshot or commit log
     * @return true if the offset has been processed, false otherwise.
     */
    boolean isOffsetProcessed(String sourceTable, String sourceOffset, boolean isSnapshot);

    /**
     * Flush latest offsets to disk.
     */
    void flush();


    /**
     * Close all resources used by this class.
     */
    void close();
}
