/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.*;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.data.Envelope;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.util.function.Function;

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
    private boolean isBeingMessage;
    private boolean isCommitMessage;
    private BufferTransactions bufferTransactions;
    private Long subTransactionId;

    public PgOutputStreamingMessageDecoder(MessageDecoderContext decoderContext, PostgresConnection connection) {
        super(decoderContext, connection);
        this.decoderContext = decoderContext;
        this.bufferTransactions = new BufferTransactions();
    }

    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, PositionLocator walPosition) {
        // Cache position as we're going to peak at the first byte to determine message type
        // We need to reprocess all BEGIN/COMMIT messages regardless.
        int position = buffer.position();
        try {
            MessageType type = MessageType.forType((char) buffer.get());

            switch (type) {
                case STREAM_START:
                    long xId = Integer.toUnsignedLong(buffer.getInt());
                    super.setTransactionId(xId);
                    return false;
                case STREAM_STOP:
                case STREAM_COMMIT:
                case STREAM_ABORT:
                    LOGGER.info("{} messages are always reprocessed", type);
                    return false;
                default:
                    buffer.position(position);
                    return super.shouldMessageBeSkipped(buffer, lastReceivedLsn, startLsn, walPosition);
            }
        }
        finally {
            // Reset buffer position
            buffer.position(position);
        }
    }

    @Override
    public void processNotEmptyMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {

        buffer.mark();
        final MessageType messageType = MessageType.forType((char) buffer.get());
        switch (messageType) {
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
                buffer.reset();
                super.processNotEmptyMessage(buffer, processor, typeRegistry);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder defaultOptions(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {

        return super.defaultOptions(builder, hasMinimumServerVersion)
                .withSlotOption("proto_version", 2)
                .withSlotOption("streaming", "on");
    }
    /**
     * Callback handler for the 'R' relation replication message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     */
    @Override
    protected void handleRelationMessage(ByteBuffer buffer, TypeRegistry typeRegistry) throws SQLException {
        long xId = extractTransactionIdFromBuffer(buffer);
        LOGGER.info("handleRelationMessage {}", xId);
        super.handleRelationMessage(buffer, typeRegistry);
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    @Override
    protected void decodeInsert(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = extractTransactionIdFromBuffer(buffer);
        LOGGER.info("decodeInsert {}", xId);
        super.decodeInsert(buffer, typeRegistry, processor);
    }
    /**
     * Callback handler for the 'U' update replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    @Override
    protected void decodeUpdate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = extractTransactionIdFromBuffer(buffer);
        LOGGER.info("decodeUpdate {}", xId);
        super.decodeUpdate(buffer, typeRegistry, processor);
    }

    /**
     * Callback handler for the 'D' delete replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    @Override
    protected void decodeDelete(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = extractTransactionIdFromBuffer(buffer);
        LOGGER.info("decodeDelete {}", xId);
        super.decodeDelete(buffer, typeRegistry, processor);
    }

    /**
     * Callback handler for the 'T' truncate replication stream message.
     *
     * @param buffer       The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor    The replication message processor
     */
    @Override
    protected void decodeTruncate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long xId = extractTransactionIdFromBuffer(buffer);
        LOGGER.info("decodeTruncate {}", xId);
        super.decodeTruncate(buffer, typeRegistry, processor);
    }

    /**
     * Callback handler for the 'M' logical decoding message
     *
     * @param buffer       The replication stream buffer
     * @param processor    The replication message processor
     */
    @Override
    protected void handleLogicalDecodingMessage(ByteBuffer buffer, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {
        long xId = extractTransactionIdFromBuffer(buffer);
        LOGGER.info("handleLogicalDecodingMessage {}", xId);
        super.handleLogicalDecodingMessage(buffer, processor);
    }

    @Override
    protected void processMessage(ReplicationMessage replicationMessage, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        immediatelyProcessMessageOrBuffer(replicationMessage, processor);
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void decodeStreamStart(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        logBufferDataIfTraceIsEnabled(buffer);
        isStreaming = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        int firstStreamSegment = buffer.get();
        super.setTransactionId(xId);
        LOGGER.info("decodeStreamStart TransactionId: {}, firstSegment: {}", xId, firstStreamSegment == 1);
        commitTimestamp = Instant.now();
        LOGGER.trace("Event: {}", MessageType.STREAM_START);
        LOGGER.trace("XID of transaction: {}", xId);
        LOGGER.trace("First Segment: {}", firstStreamSegment);
        if (firstStreamSegment == 1 || isSearchingWAL()) {
            isBeingMessage = true;
            subTransactionId = xId; // For the first stream xId will be subTransactionId
            TransactionMessage transactionMessage = new TransactionMessage(Operation.BEGIN, super.getTransactionId(), commitTimestamp);
            immediatelyProcessMessageOrBuffer(transactionMessage, processor);
        }

        isBeingMessage = false;
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     */
    private void decodeStreamStop() throws SQLException, InterruptedException {
        LOGGER.info(" decodeStreamStop Stream has stopped");
        LOGGER.trace("Stream has stopped");
        resetValuesForNextStream();
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void decodeStreamCommit(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        logBufferDataIfTraceIsEnabled(buffer);
        isCommitMessage = true;
        isStreaming = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        setTransactionId(xId);
        LOGGER.info("decodeStreamCommit XID of transaction: {}", xId);
        LOGGER.trace("XID of transaction: {}", xId);
        handleCommitMessage(buffer, processor);
        if (!isSearchingWAL()) {
            bufferTransactions.flushOnCommit(xId, processor);
        }
        resetValuesForNextStream();
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     */
    private void decodeStreamAbort(ByteBuffer buffer) {
        logBufferDataIfTraceIsEnabled(buffer);
        isStreaming = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        setTransactionId(xId);
        subTransactionId = Integer.toUnsignedLong(buffer.getInt());
        LOGGER.info("decodeStreamAbort XID of transaction: {}", getTransactionId());
        LOGGER.trace("XID of transaction: {}", getTransactionId());
        LOGGER.info("XID of sub-transaction: {}", subTransactionId);
        LOGGER.trace("XID of sub-transaction: {}", subTransactionId);
        bufferTransactions.discardSubTransaction(getTransactionId(), subTransactionId);
        resetValuesForNextStream();
    }

    private long extractTransactionIdFromBuffer(ByteBuffer buffer) {
        long xId = 0;
        if (isStreaming) {
            xId = Integer.toUnsignedLong(buffer.getInt());
            subTransactionId = xId;
            LOGGER.trace("XID of transaction: {}", xId);
        }
        return xId;
    }

    private void immediatelyProcessMessageOrBuffer(ReplicationMessage replicationMessage, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        if (isStreaming && !isSearchingWAL()) {
            if (isBeingMessage) {
                this.bufferTransactions.beginMessage(super.getTransactionId(), subTransactionId, replicationMessage);
            } else if (isCommitMessage) {
                long subTransactionId = this.subTransactionId != null ? this.subTransactionId : -1;
                this.bufferTransactions.commitMessage(super.getTransactionId(), subTransactionId, replicationMessage);
            } else {
                this.bufferTransactions.addToBuffer(super.getTransactionId(), subTransactionId, replicationMessage);
            }
        } else {
            processor.process(replicationMessage, super.getTransactionId());
        }
    }

    private void resetValuesForNextStream() {
        setTransactionId(null);
        subTransactionId = null;
        isStreaming = false;
        isBeingMessage = false;
        isCommitMessage = false;
    }

    private boolean isTruncateEventsIncluded() {
        return !decoderContext.getConfig().getSkippedOperations().contains(Envelope.Operation.TRUNCATE);
    }
}
