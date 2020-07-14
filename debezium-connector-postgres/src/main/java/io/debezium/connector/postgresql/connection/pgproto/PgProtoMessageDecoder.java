/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgproto;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.MessageDecoderConfig;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.connector.postgresql.proto.PgProto.Op;
import io.debezium.connector.postgresql.proto.PgProto.RowMessage;
import io.debezium.util.Collect;

/**
 * ProtoBuf deserialization of message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</a>.
 * Only one message is delivered for processing.
 *
 * @author Jiri Pechanec
 *
 */
public class PgProtoMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgProtoMessageDecoder.class);
    private static final Set<Op> SUPPORTED_OPS = Collect.unmodifiableSet(Op.INSERT, Op.UPDATE, Op.DELETE, Op.BEGIN, Op.COMMIT);

    private boolean warnedOnUnkownOp = false;

    public PgProtoMessageDecoder(MessageDecoderConfig config) {
        super(config);
    }

    @Override
    public void processNotEmptyMessage(final ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException(
                        "Invalid buffer received from Postgres server during streaming replication");
            }
            final byte[] source = buffer.array();
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            final RowMessage message = PgProto.RowMessage.parseFrom(content);
            LOGGER.trace("Received protobuf message from the server {}", message);
            if (!message.getNewTypeinfoList().isEmpty() && message.getNewTupleCount() != message.getNewTypeinfoCount()) {
                throw new ConnectException(String.format("Message from transaction {} has {} data columns but only {} of type info",
                        Integer.toUnsignedLong(message.getTransactionId()),
                        message.getNewTupleCount(),
                        message.getNewTypeinfoCount()));
            }
            if (!SUPPORTED_OPS.contains(message.getOp())) {
                if (!warnedOnUnkownOp) {
                    LOGGER.warn("Received message with type '{}' that is unknown to this version of connector, consider upgrading", message.getOp());
                    warnedOnUnkownOp = true;
                }
                return;
            }
            processor.process(new PgProtoReplicationMessage(message, typeRegistry));
        }
        catch (InvalidProtocolBufferException e) {
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
