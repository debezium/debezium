/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgproto;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.proto.PgProto;

/**
 * ProtoBuf deserialization of message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>.
 * The message is encapsulated into a List as ProtBuf plugin sends only one message.
 * 
 * @author Jiri Pechanec
 *
 */
public class PgProtoMessageDecoder implements MessageDecoder {

    public PgProtoMessageDecoder() {
        super();
    }

    @Override
    public List<ReplicationMessage> deserializeMessage(final ByteBuffer buffer) {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException(
                        "Invalid buffer received from PG server during streaming replication");
            }
            byte[] source = buffer.array();
            byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            return Collections.singletonList(new PgProtoReplicationMessage(PgProto.RowMessage.parseFrom(content)));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder options(ChainedLogicalStreamBuilder builder) {
        return builder;
    }
}
