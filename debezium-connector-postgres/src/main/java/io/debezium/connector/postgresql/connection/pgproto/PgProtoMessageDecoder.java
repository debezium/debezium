/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgproto;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.proto.PgProto;

/**
 * ProtoBuf deserialization of message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>.
 * Only one message is delivered for processing.
 * 
 * @author Jiri Pechanec
 *
 */
public class PgProtoMessageDecoder implements MessageDecoder {

    public PgProtoMessageDecoder() {
        super();
    }

    @Override
    public void processMessage(final ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException(
                        "Invalid buffer received from PG server during streaming replication");
            }
            byte[] source = buffer.array();
            byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            processor.process(new PgProtoReplicationMessage(PgProto.RowMessage.parseFrom(content)));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder options(ChainedLogicalStreamBuilder builder) {
        return builder;
    }
}
