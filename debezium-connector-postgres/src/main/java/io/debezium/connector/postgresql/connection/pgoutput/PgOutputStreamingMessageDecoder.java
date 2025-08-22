/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.*;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;
import io.debezium.connector.postgresql.connection.ReplicationMessage.NoopMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.data.Envelope;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.stream.Collectors.toMap;

/**
 * Decodes messages from the PG logical replication plug-in ("pgoutput").
 * See https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html for the protocol specification.z
 *
 * @author Pranav Tiwari
 *
 */
public class PgOutputStreamingMessageDecoder extends PgOutputMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgOutputStreamingMessageDecoder.class);
    private static final byte SPACE = 32;
    private final MessageDecoderContext decoderContext;
    private Instant commitTimestamp;
    private boolean isStreaming;
    private BufferTransactions bufferTransactions;
    private Long subTransactionId;

    public enum MessageType {
        RELATION,
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE,
        TYPE,
        ORIGIN,
        TRUNCATE,
        LOGICAL_DECODING_MESSAGE,
        STREAM_START,
        STREAM_STOP,
        STREAM_COMMIT,
        STREAM_ABORT;

        public static MessageType forType(char type) {
            switch (type) {
                case 'R':
                    return RELATION;
                case 'B':
                    return BEGIN;
                case 'C':
                    return COMMIT;
                case 'I':
                    return INSERT;
                case 'U':
                    return UPDATE;
                case 'D':
                    return DELETE;
                case 'Y':
                    return TYPE;
                case 'O':
                    return ORIGIN;
                case 'T':
                    return TRUNCATE;
                case 'M':
                    return LOGICAL_DECODING_MESSAGE;
                case 'S':
                    return STREAM_START;
                case 'E':
                    return STREAM_STOP;
                case 'c':
                    return STREAM_COMMIT;
                case 'A':
                    return STREAM_ABORT;
                default:
                    throw new IllegalArgumentException("Unsupported message type: " + type);
            }
        }
    }

    public PgOutputStreamingMessageDecoder(MessageDecoderContext decoderContext, PostgresConnection connection) {
        super(decoderContext, connection);
        this.decoderContext = decoderContext;
        this.bufferTransactions = new BufferTransactions();
    }

    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, WalPositionLocator walPosition) {
        // Cache position as we're going to peak at the first byte to determine message type
        // We need to reprocess all BEGIN/COMMIT messages regardless.
        int position = buffer.position();
        try {
            MessageType type = MessageType.forType((char) buffer.get());
            // LOGGER.info("Message Type: {}", type);
            switch (type) {
                case TYPE:
                case ORIGIN:
                    // TYPE/ORIGIN
                    // These should be skipped without calling shouldMessageBeSkipped. DBZ-5792
                    LOGGER.info("{} messages are always skipped without calling shouldMessageBeSkipped", type);
                    return true;
                case TRUNCATE:
                    if (!isTruncateEventsIncluded()) {
                        LOGGER.info("{} messages are being skipped without calling shouldMessageBeSkipped", type);
                        return true;
                    }
                    // else delegate to super.shouldMessageBeSkipped
                    break;
                default:
                    // call super.shouldMessageBeSkipped for rest of the types
            }
            final boolean candidateForSkipping = false;
            switch (type) {
                case COMMIT:
                case BEGIN:
                case RELATION:
                case STREAM_START:
                case STREAM_STOP:
                case STREAM_COMMIT:
                case STREAM_ABORT:
                    // BEGIN
                    // These types should always be processed due to the nature that they provide
                    // the stream with pertinent per-transaction boundary state we will need to
                    // always cache as we potentially might reprocess the stream from an earlier
                    // LSN point.
                    //
                    // RELATION
                    // These messages are always sent with a lastReceivedLSN=0; and we need to
                    // always accept these to keep per-stream table state cached properly.
                    LOGGER.info("{} messages are always reprocessed", type);
                    return false;
                default:
                    // INSERT/UPDATE/DELETE/TRUNCATE/LOGICAL_DECODING_MESSAGE
                    // These should be excluded based on the normal behavior, delegating to default method
                    return candidateForSkipping;
            }
        }
        finally {
            // Reset buffer position
            buffer.position(position);
        }
    }

    @Override
    public void processNotEmptyMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        if (LOGGER.isTraceEnabled()) {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            // Extend the array by two as we might need to append two chars and set them to space by default
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length + 2);
            final int lastPos = content.length - 1;
            content[lastPos - 1] = SPACE;
            content[lastPos] = SPACE;
            LOGGER.info("Message arrived from database {}", HexConverter.convertToHexString(content));
        }

        final MessageType messageType = MessageType.forType((char) buffer.get());
        switch (messageType) {
            case BEGIN:
                super.handleBeginMessage(buffer, processor);
                break;
            case COMMIT:
                super.handleCommitMessage(buffer, processor);
                break;
            case RELATION:
                handleRelationMessage(buffer, typeRegistry);
                break;
            case LOGICAL_DECODING_MESSAGE:
                handleLogicalDecodingMessage(buffer, processor);
                break;
            case INSERT:
                decodeInsert(buffer, typeRegistry, processor);
                break;
            case UPDATE:
                decodeUpdate(buffer, typeRegistry, processor);
                break;
            case DELETE:
                decodeDelete(buffer, typeRegistry, processor);
                break;
            case TRUNCATE:
                if (isTruncateEventsIncluded()) {
                    decodeTruncate(buffer, typeRegistry, processor);
                }
                else {
                    LOGGER.info("Message Type {} skipped, not processed.", messageType);
                }
                break;
            case STREAM_START:
                decodeStreamStart(buffer, processor);
                break;
            case STREAM_STOP:
                decodeStreamStop();
                break;
            case STREAM_COMMIT:
                decodeStreamCommit(buffer, processor);
                break;
            case STREAM_ABORT:
                decodeStreamAbort(buffer);
                break;
            default:
                LOGGER.info("Message Type {} skipped, not processed.", messageType);
                break;
        }
    }

    @Override
    public ChainedLogicalStreamBuilder defaultOptions(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {
        builder = builder.withSlotOption("proto_version", 2)
                .withSlotOption("streaming", "on")
                .withSlotOption("publication_names", decoderContext.getConfig().publicationName());

        // DBZ-4374 Use enum once the driver got updated
        if (hasMinimumServerVersion.apply(140000)) {
            builder = builder.withSlotOption("messages", true);
        }

        return builder;
    }

    private boolean isTruncateEventsIncluded() {
        return !decoderContext.getConfig().getSkippedOperations().contains(Envelope.Operation.TRUNCATE);
    }

    /**
     * Callback handler for the 'R' relation replication message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     */
    protected void handleRelationMessage(ByteBuffer buffer, TypeRegistry typeRegistry) throws SQLException {
        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            LOGGER.trace("XID of transaction: {}", xId);
        }

        super.handleRelationMessage(buffer, typeRegistry);
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    protected void decodeInsert(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            subTransactionId = xId;
            LOGGER.trace("XID of transaction: {}", xId);
        }
        super.decodeInsert(buffer, typeRegistry, processor);
    }
    /**
     * Callback handler for the 'U' update replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    protected void decodeUpdate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            subTransactionId = xId;
            LOGGER.trace("XID of transaction: {}", xId);
        }

        super.decodeUpdate(buffer, typeRegistry, processor);
    }

    /**
     * Callback handler for the 'D' delete replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    protected void decodeDelete(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            subTransactionId = xId;
            LOGGER.trace("XID of transaction: {}", xId);
        }

        super.decodeDelete(buffer, typeRegistry, processor);
    }

    /**
     * Callback handler for the 'T' truncate replication stream message.
     *
     * @param buffer       The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor    The replication message processor
     */
    protected void decodeTruncate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {

        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            subTransactionId = xId;
            LOGGER.trace("XID of transaction: {}", xId);
        }

        super.decodeTruncate(buffer, typeRegistry, processor);
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void decodeStreamStart(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        isStreaming = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        int firstStreamSegment = buffer.get();

        super.setTransactionId(xId);
        subTransactionId = xId;

        LOGGER.info("TransactionId: {}, firstSegment: {}", xId, firstStreamSegment == 1);

        commitTimestamp = Instant.now();

        LOGGER.trace("Event: {}", MessageType.STREAM_START);
        LOGGER.trace("XID of transaction: {}", xId);
        LOGGER.trace("First Segment: {}", firstStreamSegment);

        if (firstStreamSegment == 1) {
            TransactionMessage transactionMessage = new TransactionMessage(Operation.BEGIN, super.getTransactionId(), commitTimestamp);
            immediatelyProcessMessageOrBuffer(transactionMessage, processor);
        }
    }


    /**
     * Callback handler for the 'I' insert replication stream message.
     */
    private void decodeStreamStop() throws SQLException, InterruptedException {
        LOGGER.trace("Stream has stopped");
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void decodeStreamCommit(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = Integer.toUnsignedLong(buffer.getInt());
        LOGGER.trace("XID of transaction: {}", xId);

        handleCommitMessage(buffer, processor);
        bufferTransactions.flushOnCommit(xId, processor);
        isStreaming = false;
        subTransactionId = null;
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     */
    private void decodeStreamAbort(ByteBuffer buffer) {

        long xId = Integer.toUnsignedLong(buffer.getInt());
        long subXId = Integer.toUnsignedLong(buffer.getInt());

        LOGGER.trace("XID of transaction: {}", xId);
        LOGGER.trace("XID of sub-transaction: {}", subXId);

        bufferTransactions.discardSubTransaction(xId, subXId);

        subTransactionId = null;
        isStreaming = false;
    }



    /**
     * Callback handler for the 'M' logical decoding message
     *
     * @param buffer       The replication stream buffer
     * @param processor    The replication message processor
     */
    protected void handleLogicalDecodingMessage(ByteBuffer buffer, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {

        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            subTransactionId = xId;
            LOGGER.trace("XID of transaction: {}", xId);
        }

        super.handleLogicalDecodingMessage(buffer, processor);
    }

    @Override
    protected void processMessage(ReplicationMessage replicationMessage, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        immediatelyProcessMessageOrBuffer(replicationMessage, processor);
    }

    private void immediatelyProcessMessageOrBuffer(ReplicationMessage replicationMessage, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        if (isStreaming) {
            this.bufferTransactions.addToBuffer(super.getTransactionId(), subTransactionId, replicationMessage);
        } else {
            processor.process(replicationMessage);
        }
    }
}
