/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGmoney;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;

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
        INSERT,
        UPDATE,
        DELETE
    }

    /**
     * A representation of column value delivered as a part of replication message
     */
    public interface Column {
        String getName();

        PostgresType getType();

        /**
         * Returns additional metadata about this column's type. May only be called
         * after checking {@link ReplicationMessage#hasMetadata()}.
         */
        ColumnTypeMetadata getTypeMetadata();

        Object getValue(final PgConnectionSupplier connection, boolean includeUnknownDatatypes);

        boolean isOptional();

        default boolean isToastedColumn() {
            return false;
        }
    }

    public interface ColumnTypeMetadata {
        int getLength();

        int getScale();
    }

    public interface ColumnValue<T> {
        T getRawValue();

        boolean isNull();

        String asString();

        Boolean asBoolean();

        Integer asInteger();

        Long asLong();

        Float asFloat();

        Double asDouble();

        Object asDecimal();

        LocalDate asLocalDate();

        OffsetDateTime asOffsetDateTimeAtUtc();

        Instant asInstant();

        Object asTime();

        Object asLocalTime();

        OffsetTime asOffsetTimeUtc();

        byte[] asByteArray();

        PGbox asBox();

        PGcircle asCircle();

        Object asInterval();

        PGline asLine();

        Object asLseg();

        PGmoney asMoney();

        PGpath asPath();

        PGpoint asPoint();

        PGpolygon asPolygon();

        boolean isArray(PostgresType type);

        Object asArray(String columnName, PostgresType type, String fullType, PgConnectionSupplier connection);

        Object asDefault(TypeRegistry typeRegistry, int columnType, String columnName, String fullType, boolean includeUnknownDatatypes, PgConnectionSupplier connection);
    }

    /**
     * @return A data operation executed
     */
    public Operation getOperation();

    /**
     * @return Transaction commit time for this change
     */
    public Instant getCommitTime();

    /**
     * @return An id of transaction to which this change belongs
     */
    public long getTransactionId();

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
    boolean hasTypeMetadata();

    /**
     * @return true if this is the last message in the batch of messages with same LSN
     */
    boolean isLastEventForLsn();

    /**
     * @return true if the stream producer should synchronize the schema when processing messages, false otherwise
     */
    default boolean shouldSchemaBeSynchronized() {
        return true;
    }
}
