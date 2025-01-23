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
import java.util.OptionalLong;

import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
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
    enum Operation {
        INSERT,
        UPDATE,
        DELETE,
        TRUNCATE,
        MESSAGE,
        BEGIN,
        COMMIT
    }

    /**
     * A representation of column value delivered as a part of replication message
     */
    interface Column {
        String getName();

        PostgresType getType();

        /**
         * Returns additional metadata about this column's type.
         */
        ColumnTypeMetadata getTypeMetadata();

        Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes);

        boolean isOptional();

        default boolean isToastedColumn() {
            return false;
        }
    }

    interface ColumnTypeMetadata {
        int getLength();

        int getScale();
    }

    interface ColumnValue<T> {
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

        Object asMoney();

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
    Operation getOperation();

    /**
     * @return Transaction commit time for this change
     */
    Instant getCommitTime();

    /**
     * @return An id of transaction to which this change belongs; will not be
     *         present for non-transactional logical decoding messages for instance
     */
    OptionalLong getTransactionId();

    /**
     * @return Table changed
     */
    String getTable();

    /**
     * @return Set of original values of table columns, null for INSERT
     */
    List<Column> getOldTupleList();

    /**
     * @return Set of new values of table columns, null for DELETE
     */
    List<Column> getNewTupleList();

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

    /**
     * Whether this message represents the begin or end of a transaction.
     */
    default boolean isTransactionalMessage() {
        return getOperation() == Operation.BEGIN || getOperation() == Operation.COMMIT;
    }

    default boolean isFiltered() {
        return false;
    }

    /**
     * Returns true if the message is a skipped message.
     */
    default boolean isSkippedMessage() {
        return false;
    }

    /**
     * A special message type that is used to replace event filtered already at {@link MessageDecoder}.
     * Enables {@link PostgresStreamingChangeEventSource} to advance LSN forward even in case of such messages.
     */
    class NoopMessage implements ReplicationMessage {

        private final Long transactionId;
        private final Instant commitTime;
        private final Operation operation;

        public NoopMessage(Long transactionId, Instant commitTime, Operation operation) {
            this.operation = operation;
            this.transactionId = transactionId;
            this.commitTime = commitTime;
        }

        @Override
        public boolean isLastEventForLsn() {
            return true;
        }

        @Override
        public OptionalLong getTransactionId() {
            return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
        }

        @Override
        public String getTable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        public List<Column> getOldTupleList() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Column> getNewTupleList() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant getCommitTime() {
            return commitTime;
        }

        @Override
        public boolean isSkippedMessage() {
            return true;
        }
    }
}
