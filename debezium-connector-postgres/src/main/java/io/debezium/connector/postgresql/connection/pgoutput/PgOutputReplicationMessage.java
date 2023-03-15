/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessageColumnValueResolver;

/**
 * @author Gunnar Morling
 * @author Chris Cranford
 */
public class PgOutputReplicationMessage implements ReplicationMessage {

    private Operation op;
    private Instant commitTimestamp;
    private Long transactionId;
    private String table;
    private List<Column> oldColumns;
    private List<Column> newColumns;

    public PgOutputReplicationMessage(Operation op, String table, Instant commitTimestamp, Long transactionId, List<Column> oldColumns, List<Column> newColumns) {
        this.op = op;
        this.commitTimestamp = commitTimestamp;
        this.transactionId = transactionId;
        this.table = table;
        this.oldColumns = oldColumns;
        this.newColumns = newColumns;
    }

    @Override
    public Operation getOperation() {
        return op;
    }

    @Override
    public Instant getCommitTime() {
        return commitTimestamp;
    }

    @Override
    public OptionalLong getTransactionId() {
        return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    @Override
    public boolean shouldSchemaBeSynchronized() {
        return false;
    }

    /**
     * Converts the value (string representation) coming from PgOutput plugin to
     * a Java value based on the type of the column from the message.  This value will be converted later on if necessary by the
     * connector's value converter to match whatever the Connect schema type expects.
     *
     * Note that the logic here is tightly coupled on the pgoutput plugin logic which writes the actual value.
     *
     * @return the value; may be null
     */
    public static Object getValue(String columnName, PostgresType type, String fullType, String rawValue, final PgConnectionSupplier connection,
                                  boolean includeUnknownDataTypes, TypeRegistry typeRegistry, PostgresConnectorConfig.TimezoneHandlingMode timezoneHandlingMode) {
        final PgOutputColumnValue columnValue = new PgOutputColumnValue(rawValue);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, connection, includeUnknownDataTypes, typeRegistry,
                timezoneHandlingMode);
    }
}
