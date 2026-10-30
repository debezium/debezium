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
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
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

    /**
     * The buffer the {@link #parsedMessage} was parsed from. The skip decision sets the two fields
     * and the processing of the message reads them, so that a message is parsed only once. The two
     * fields are always set and cleared together, so a buffer that is set has a message with it.
     * One decoder serves one replication connection, read by the single thread of the connector
     * task, so the two fields need no synchronization.
     */
    private ByteBuffer parsedBuffer;

    /**
     * The message parsed from {@link #parsedBuffer}, kept only while the message is between the
     * skip decision and its processing.
     */
    private RowMessage parsedMessage;

    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, WalPositionLocator walPosition) {
        if (buffer == null) {
            // The replication stream returns no buffer when it received no message, so there is no
            // operation to read and the decision is the one the base class makes.
            return super.shouldMessageBeSkipped(buffer, lastReceivedLsn, startLsn, walPosition);
        }
        final RowMessage message = parseMessage(buffer);
        parsedBuffer = buffer;
        parsedMessage = message;
        final boolean skipped = switch (message.getOp()) {
            case BEGIN, COMMIT -> {
                // BEGIN and COMMIT are not events lsn_events_processed counts, so they must not use
                // up a skip meant for the already-processed rows at the stored LSN. They are always
                // reprocessed because they carry the per-transaction boundary state.
                walPosition.skipMessage(lastReceivedLsn, false);
                yield false;
            }
            case INSERT, UPDATE, DELETE -> super.shouldMessageBeSkipped(buffer, lastReceivedLsn, startLsn, walPosition);
            // An operation this version does not know is not an event lsn_events_processed
            // counts, so it must not use up a skip meant for the already-processed rows at the
            // stored LSN.
            default -> walPosition.skipMessage(lastReceivedLsn, false);
        };
        if (skipped) {
            // The message is not processed, so the message parsed for the decision is not needed
            // any more.
            parsedBuffer = null;
            parsedMessage = null;
        }
        return skipped;
    }

    @Override
    public void processNotEmptyMessage(final ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        // The skip decision parsed the message of this buffer, so it is parsed only once. A
        // message that was not decided upon by the skip decision is parsed here.
        final RowMessage message = buffer == parsedBuffer ? parsedMessage : parseMessage(buffer);
        parsedBuffer = null;
        parsedMessage = null;
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

    @Override
    public ChainedLogicalStreamBuilder defaultOptions(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    private RowMessage parseMessage(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalStateException(
                    "Invalid buffer received from Postgres server during streaming replication");
        }
        final byte[] source = buffer.array();
        final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
        final RowMessage message;
        try {
            message = PgProto.RowMessage.parseFrom(content);
        }
        catch (InvalidProtocolBufferException e) {
            throw new ConnectException(e);
        }
        return message;
    }
}
