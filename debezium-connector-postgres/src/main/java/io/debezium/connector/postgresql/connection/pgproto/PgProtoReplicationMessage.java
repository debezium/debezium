/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.pgproto;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessageColumnValueResolver;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.connector.postgresql.proto.PgProto.Op;
import io.debezium.util.Strings;

/**
 * Replication message representing message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>
 *
 * @author Jiri Pechanec
 */
class PgProtoReplicationMessage implements ReplicationMessage {

    private final PgProto.RowMessage rawMessage;
    private final TypeRegistry typeRegistry;

    PgProtoReplicationMessage(PgProto.RowMessage rawMessage, TypeRegistry typeRegistry) {
        this.rawMessage = rawMessage;
        this.typeRegistry = typeRegistry;

        if (missingTypeMetadata()) {
            throw new DebeziumException("Protobuf message does not contain metadata. Unsupported version of protobuf plug-in is deployed in the database.");
        }
    }

    @Override
    public Operation getOperation() {
        switch (rawMessage.getOp()) {
            case INSERT:
                return Operation.INSERT;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            case BEGIN:
                return Operation.BEGIN;
            case COMMIT:
                return Operation.COMMIT;
            default:
                throw new IllegalArgumentException("Unknown operation '" + rawMessage.getOp() + "' in replication stream message");
        }
    }

    @Override
    public Instant getCommitTime() {
        // value is microseconds
        return Instant.ofEpochSecond(0, rawMessage.getCommitTime() * 1_000);
    }

    @Override
    public OptionalLong getTransactionId() {
        return OptionalLong.of(Integer.toUnsignedLong(rawMessage.getTransactionId()));
    }

    @Override
    public String getTable() {
        return rawMessage.getTable();
    }

    @Override
    public List<ReplicationMessage.Column> getOldTupleList() {
        return transform(rawMessage.getOldTupleList(), null);
    }

    @Override
    public List<ReplicationMessage.Column> getNewTupleList() {
        return transform(rawMessage.getNewTupleList(), rawMessage.getNewTypeinfoList());
    }

    private boolean missingTypeMetadata() {
        if (rawMessage.getOp() == Op.BEGIN || rawMessage.getOp() == Op.COMMIT || rawMessage.getOp() == Op.DELETE) {
            return false;
        }
        return rawMessage.getNewTypeinfoList() == null;
    }

    private List<ReplicationMessage.Column> transform(List<PgProto.DatumMessage> messageList, List<PgProto.TypeInfo> typeInfoList) {
        return IntStream.range(0, messageList.size())
                .mapToObj(index -> {
                    final PgProto.DatumMessage datum = messageList.get(index);
                    final Optional<PgProto.TypeInfo> typeInfo = Optional.ofNullable(typeInfoList != null ? typeInfoList.get(index) : null);
                    final String columnName = Strings.unquoteIdentifierPart(datum.getColumnName());
                    final PostgresType type = typeRegistry.get((int) datum.getColumnType());
                    if (datum.hasDatumMissing()) {
                        return new UnchangedToastedReplicationMessageColumn(columnName, type, typeInfo.map(PgProto.TypeInfo::getModifier).orElse(null),
                                typeInfo.map(PgProto.TypeInfo::getValueOptional).orElse(Boolean.FALSE));
                    }

                    final String fullType = typeInfo.map(PgProto.TypeInfo::getModifier).orElse(null);
                    return new AbstractReplicationMessageColumn(columnName, type, fullType,
                            typeInfo.map(PgProto.TypeInfo::getValueOptional).orElse(Boolean.FALSE)) {

                        @Override
                        public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes,
                                               PostgresConnectorConfig.TimezoneHandlingMode timezoneHandlingMode) {
                            return PgProtoReplicationMessage.this.getValue(columnName, type, fullType, datum, connection, includeUnknownDatatypes, timezoneHandlingMode);
                        }

                        @Override
                        public String toString() {
                            return datum.toString();
                        }
                    };
                })
                .collect(Collectors.toList());
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    public Object getValue(String columnName, PostgresType type, String fullType, PgProto.DatumMessage datumMessage, final PgConnectionSupplier connection,
                           boolean includeUnknownDatatypes, PostgresConnectorConfig.TimezoneHandlingMode timezoneHandlingMode) {
        final PgProtoColumnValue columnValue = new PgProtoColumnValue(datumMessage);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, connection, includeUnknownDatatypes, typeRegistry,
                timezoneHandlingMode);
    }

    @Override
    public String toString() {
        return "PgProtoReplicationMessage [rawMessage=" + rawMessage + "]";
    }
}
