/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.util.List;
import java.util.OptionalInt;

import io.debezium.connector.postgresql.RecordsStreamProducer.PgConnectionSupplier;

/**
 * An abstract representation of a replication message that is sent by a PostgreSQL logical decoding plugin and
 * is processed by the Debezium PostgreSQL connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface ReplicationMessage {

    /**
     *
     * Data modification operation executed
     *
     */
    public enum Operation {
        INSERT, UPDATE, DELETE
    }

    /**
     * A representation of column value delivered as a part of replication message
     */
    public interface Column {
        String getName();
        int getOidType();

        /**
         * Returns the type of array elements if this column is of an array type. May
         * only be called after checking {@link ReplicationMessage#hasMetadata()}.
         */
        int getComponentOidType();

        /**
         * Returns additional metadata about this column's type. May only be called
         * after checking {@link ReplicationMessage#hasMetadata()}.
         */
        ColumnTypeMetadata getTypeMetadata();
        Object getValue(final PgConnectionSupplier connection, boolean includeUnknownDatatypes);
        boolean isOptional();
    }

    public interface ColumnTypeMetadata {
        String getName();
        OptionalInt getLength();
        OptionalInt getScale();
        boolean isArray();
    }

    /**
     * @return A data operation executed
     */
    public Operation getOperation();

    /**
     * @return Transaction commit time for this change
     */
    public long getCommitTime();

    /**
     * @return An id of transaction to which this change belongs
     */
    public int getTransactionId();

    /**
     * @return Table changed
     */
    public String getTable();

    /**
     * @return Set of original values of table columns, null for INSERT
     */
    public List<Column> getOldTupleList();

    /**
     * @return Set of new values of table columns, null for DELETE
     */
    public List<Column> getNewTupleList();

    /**
     * @return true if type metadata are passed as a part of message
     */
    boolean hasMetadata();
}
