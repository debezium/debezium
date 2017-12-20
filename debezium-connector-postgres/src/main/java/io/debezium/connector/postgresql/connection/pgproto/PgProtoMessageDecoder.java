/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgproto;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.connector.postgresql.proto.PgProto.RowMessage;

/**
 * ProtoBuf deserialization of message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>.
 * Only one message is delivered for processing.
 *
 * @author Jiri Pechanec
 *
 */
public class PgProtoMessageDecoder implements MessageDecoder {

    @Override
    public void processMessage(final ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException(
                        "Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            final RowMessage message = PgProto.RowMessage.parseFrom(content);
            if (!message.getNewTypeinfoList().isEmpty() && message.getNewTupleCount() != message.getNewTypeinfoCount()) {
                throw new ConnectException(String.format("Message from transaction {} has {} data columns but only {} of type info",
                        message.getTransactionId(),
                        message.getNewTupleCount(),
                        message.getNewTypeinfoCount()));
            }
            processor.process(new PgProtoReplicationMessage(message));
        } catch (InvalidProtocolBufferException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }
}
