/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.util.function.Function;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.MessageDecoderContext;
import io.debezium.connector.postgresql.connection.PositionLocator;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.TransactionMessage;

/**
 * Decodes messages from the PG logical replication plug-in ("pgoutput").
 * See https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html for the protocol specification.z
 *
 * @author Pranav Tiwari
 *
 */
public class PgOutputStreamingMessageDecoder extends PgOutputMessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgOutputStreamingMessageDecoder.class);
    private Instant commitTimestamp;
    private boolean isStreaming;
    private boolean isBeginMessage;
    private boolean isCommitMessage;
    private BufferTransactions bufferTransactions;
    private Long subTransactionId;

    public PgOutputStreamingMessageDecoder(MessageDecoderContext decoderContext, PostgresConnection connection) {
        super(decoderContext, connection);
        this.bufferTransactions = new BufferTransactions();
    }

    /**
     * Determines whether the replication message in the given {@link ByteBuffer} should be skipped
     * during processing. This is typically used to avoid reprocessing already-handled messages.
     * <p>
     * Certain message types (e.g., {@code STREAM_START}, {@code STREAM_STOP}, {@code STREAM_COMMIT},
     * {@code STREAM_ABORT}) are always reprocessed regardless of the current replication position.
     * All other message types delegate the decision to the superclass method.
     * </p>
     *
     * @param buffer          the {@link ByteBuffer} containing the message to inspect; must not be null.
     * @param lastReceivedLsn the last received Log Sequence Number (LSN); used to determine whether the message is new.
     * @param startLsn        the start LSN from which replication began.
     * @param walPosition     the {@link PositionLocator} used to resolve WAL (Write-Ahead Log) positions.
     * @return {@code true} if the message can be safely skipped; {@code false} if it should be processed.
     */
    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, PositionLocator walPosition) {
        // Cache position as we're going to peak at the first byte to determine message type
        // We need to reprocess all BEGIN/COMMIT messages regardless.
        int position = buffer.position();
        try {
            MessageType type = MessageType.forType((char) buffer.get());

            switch (type) {
                case STREAM_START:
                    LOGGER.info("{} messages are always reprocessed", type);
                    long xId = Integer.toUnsignedLong(buffer.getInt());
                    LOGGER.trace("Setting transaction id to {}", xId);
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

    /**
     * Processes a non-empty replication message from the given {@link ByteBuffer}.
     * <p>
     * This method reads the first byte of the buffer to determine the message type
     * and dispatches the buffer to the appropriate decoding method. If the message
     * type is unrecognized, the buffer is reset and the superclass's implementation
     * of {@code processNotEmptyMessage} is invoked.
     * </p>
     *
     * @param buffer         the {@link ByteBuffer} containing the replication message; must not be null.
     * @param processor      the {@link ReplicationMessageProcessor} that handles the decoded message logic; must not be null.
     * @param typeRegistry   the {@link TypeRegistry} used for type resolution; must not be null.
     * @throws SQLException              if a database access error occurs while processing the message.
     * @throws InterruptedException      if the thread is interrupted while processing the message.
     */
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

    /**
     * Applies default logical replication slot options to the provided {@link ChainedLogicalStreamBuilder}.
     * <p>
     * This implementation sets the {@code proto_version} to {@code 2} and enables {@code streaming} by setting it to {@code "on"}.
     * It delegates to the superclass implementation first, and then adds or overrides options as needed.
     * </p>
     *
     * @param builder                 the {@link ChainedLogicalStreamBuilder} used to configure the logical replication stream; must not be null.
     * @param hasMinimumServerVersion a function to check if a given server version is supported; must not be null.
     * @return the updated {@link ChainedLogicalStreamBuilder} with default options applied.
     */
    @Override
    public ChainedLogicalStreamBuilder defaultOptions(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {

        return super.defaultOptions(builder, hasMinimumServerVersion)
                .withSlotOption("proto_version", 2)
                .withSlotOption("streaming", "on");
    }

    /**
     * Handles a {@code RELATION} replication message.
     * <p>
     * This implementation first extracts the sub-transaction ID from the given {@link ByteBuffer},
     * then delegates the remaining message processing to the superclass.
     * </p>
     *
     * @param buffer        the {@link ByteBuffer} containing the relation message data; must not be null.
     * @param typeRegistry  the {@link TypeRegistry} used to resolve data types in the relation; must not be null.
     * @throws SQLException if a database access error occurs during processing.
     */
    @Override
    protected void handleRelationMessage(ByteBuffer buffer, TypeRegistry typeRegistry) throws SQLException {
        extractSubTransactionIdFromBuffer(buffer);
        super.handleRelationMessage(buffer, typeRegistry);
    }

    /**
     * Decodes an {@code INSERT} replication message from the given {@link ByteBuffer}.
     * <p>
     * This implementation first extracts the sub-transaction ID from the buffer,
     * then delegates the rest of the insert message decoding to the superclass.
     * </p>
     *
     * @param buffer        the {@link ByteBuffer} containing the insert message data; must not be null.
     * @param typeRegistry  the {@link TypeRegistry} used to resolve column data types; must not be null.
     * @param processor     the {@link ReplicationMessageProcessor} that handles the decoded message; must not be null.
     * @throws SQLException           if a database access error occurs while decoding.
     * @throws InterruptedException   if the thread is interrupted during processing.
     */
    @Override
    protected void decodeInsert(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        extractSubTransactionIdFromBuffer(buffer);
        super.decodeInsert(buffer, typeRegistry, processor);
    }

    /**
     * Decodes an {@code UPDATE} replication message from the given {@link ByteBuffer}.
     * <p>
     * This implementation first extracts the sub-transaction ID from the buffer,
     * then delegates the decoding of the update message to the superclass.
     * </p>
     *
     * @param buffer        the {@link ByteBuffer} containing the update message data; must not be null.
     * @param typeRegistry  the {@link TypeRegistry} used to resolve column data types; must not be null.
     * @param processor     the {@link ReplicationMessageProcessor} that handles the decoded message; must not be null.
     * @throws SQLException           if a database access error occurs while decoding.
     * @throws InterruptedException   if the thread is interrupted during processing.
     */
    @Override
    protected void decodeUpdate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        extractSubTransactionIdFromBuffer(buffer);
        super.decodeUpdate(buffer, typeRegistry, processor);
    }

    /**
     * Decodes a {@code DELETE} replication message from the given {@link ByteBuffer}.
     * <p>
     * This implementation begins by extracting the sub-transaction ID from the buffer,
     * and then delegates the actual decoding of the delete message to the superclass.
     * </p>
     *
     * @param buffer        the {@link ByteBuffer} containing the delete message data; must not be null.
     * @param typeRegistry  the {@link TypeRegistry} used to resolve column data types; must not be null.
     * @param processor     the {@link ReplicationMessageProcessor} that handles the decoded message; must not be null.
     * @throws SQLException           if a database access error occurs while decoding.
     * @throws InterruptedException   if the thread is interrupted during processing.
     */
    @Override
    protected void decodeDelete(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        extractSubTransactionIdFromBuffer(buffer);
        super.decodeDelete(buffer, typeRegistry, processor);
    }

    /**
     * Decodes a {@code TRUNCATE} replication message from the given {@link ByteBuffer}.
     * <p>
     * This implementation first extracts the sub-transaction ID from the buffer,
     * then delegates the decoding of the truncate message to the superclass.
     * </p>
     *
     * @param buffer        the {@link ByteBuffer} containing the truncate message data; must not be null.
     * @param typeRegistry  the {@link TypeRegistry} used to resolve relation and column types; must not be null.
     * @param processor     the {@link ReplicationMessageProcessor} that processes the decoded message; must not be null.
     * @throws SQLException           if a database access error occurs during decoding.
     * @throws InterruptedException   if the thread is interrupted during processing.
     */
    @Override
    protected void decodeTruncate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        extractSubTransactionIdFromBuffer(buffer);
        super.decodeTruncate(buffer, typeRegistry, processor);
    }

    /**
     * Handles a logical decoding message from the provided {@link ByteBuffer}.
     * <p>
     * This implementation first extracts the sub-transaction ID from the buffer,
     * then delegates the processing of the logical decoding message to the superclass.
     * </p>
     *
     * @param buffer     the {@link ByteBuffer} containing the logical decoding message; must not be null.
     * @param processor  the {@link ReplicationMessageProcessor} that processes the decoded message; must not be null.
     * @throws SQLException           if a database access error occurs during processing.
     * @throws InterruptedException   if the thread is interrupted while processing.
     */
    @Override
    protected void handleLogicalDecodingMessage(ByteBuffer buffer, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {
        extractSubTransactionIdFromBuffer(buffer);
        super.handleLogicalDecodingMessage(buffer, processor);
    }

    @Override
    protected void processMessage(ReplicationMessage replicationMessage, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        immediatelyProcessMessageOrBuffer(replicationMessage, processor);
    }

    /**
     * Decodes a {@code STREAM_START} replication message from the provided {@link ByteBuffer}.
     * <p>
     * This method reads the transaction ID and the first stream segment indicator from the buffer,
     * updates the streaming state.
     * If this is the first stream segment or if the system is currently searching the WAL (Write-Ahead Log),
     * it creates and immediately processes a {@link TransactionMessage} with an {@code BEGIN} operation.
     * </p>
     *
     * @param buffer    the {@link ByteBuffer} containing the stream start message data; must not be null.
     * @param processor the {@link ReplicationMessageProcessor} responsible for handling decoded messages; must not be null.
     * @throws SQLException           if a database access error occurs while processing.
     * @throws InterruptedException   if the thread is interrupted during processing.
     */
    private void decodeStreamStart(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        logBufferDataIfTraceIsEnabled(buffer);
        isStreaming = true;
        isBeginMessage = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        int firstStreamSegment = buffer.get();
        setTransactionId(xId);
        commitTimestamp = Instant.now();
        LOGGER.trace("Stream has started for transaction-id {} and is first batch = {}", getTransactionId(), firstStreamSegment == 1);
        LOGGER.trace("Event: {}", MessageType.STREAM_START);
        LOGGER.trace("XID of transaction: {}", xId);
        LOGGER.trace("First Segment: {}", firstStreamSegment);
        if (firstStreamSegment == 1 || isSearchingWAL()) {
            isBeginMessage = true;
            subTransactionId = xId; // For the first stream xId will be subTransactionId
            TransactionMessage transactionMessage = new TransactionMessage(Operation.BEGIN, super.getTransactionId(), commitTimestamp);
            immediatelyProcessMessageOrBuffer(transactionMessage, processor);
        }

        isBeginMessage = false;
    }

    /**
     * Handles the {@code STREAM_STOP} replication event.
     * <p>
     * Resets internal state values in preparation for the next stream.
     * </p>
     */
    private void decodeStreamStop() {
        LOGGER.trace("Received stream stop event for sub-transaction id: {} of transaction id: {}", subTransactionId, getTransactionId());
        resetValuesForNextStream();
    }

    /**
     * Decodes a {@code STREAM_COMMIT} replication message from the provided {@link ByteBuffer}.
     * <p>
     * This method marks the current message as a commit message and updates streaming state.
     * It reads the transaction ID from the buffer, sets it.
     * The commit message is then handled, and if the system is not currently searching for
     * a WAL (Write-Ahead Log) position, buffered transactions are flushed.
     * Finally, internal state values are reset to prepare for the next stream.
     * </p>
     *
     * @param buffer    the {@link ByteBuffer} containing the stream commit message data; must not be null.
     * @param processor the {@link ReplicationMessageProcessor} responsible for processing replication messages; must not be null.
     * @throws SQLException           if a database access error occurs during processing.
     * @throws InterruptedException   if the thread is interrupted during processing.
     */
    private void decodeStreamCommit(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        logBufferDataIfTraceIsEnabled(buffer);
        isCommitMessage = true;
        isStreaming = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        setTransactionId(xId);
        LOGGER.trace("Received stream commit event of transaction id: {}", getTransactionId());
        LOGGER.trace("XID of transaction: {}", getTransactionId());
        handleCommitMessage(buffer, processor);
        if (!isSearchingWAL()) { // We will trigger flush only when we are not searching for WAL location.
            bufferTransactions.flushOnCommit(xId, processor);
        }
        resetValuesForNextStream();
    }

    /**
     * Decodes a {@code STREAM_ABORT} replication message from the given {@link ByteBuffer}.
     * <p>
     * This method marks streaming as active, reads the transaction ID and sub-transaction ID from the buffer,
     * logs relevant details at trace level, and discards all buffered messages related to the sub-transaction.
     * Finally, it resets internal state values to prepare for the next stream.
     * </p>
     *
     * @param buffer the {@link ByteBuffer} containing the stream abort message data; must not be null.
     */
    private void decodeStreamAbort(ByteBuffer buffer) {
        logBufferDataIfTraceIsEnabled(buffer);
        isStreaming = true;
        long xId = Integer.toUnsignedLong(buffer.getInt());
        setTransactionId(xId);
        subTransactionId = Integer.toUnsignedLong(buffer.getInt());
        LOGGER.trace("XID of transaction: {}", getTransactionId());
        LOGGER.trace("XID of sub-transaction: {}", subTransactionId);
        LOGGER.trace("Received stream abort event for sub-transaction: {} of transaction id: {}. Removing all messages of the sub-transaction id.", subTransactionId,
                getTransactionId());
        bufferTransactions.discardSubTransaction(getTransactionId(), subTransactionId);
        resetValuesForNextStream();
    }

    private Long extractSubTransactionIdFromBuffer(ByteBuffer buffer) {
        if (isStreaming) {
            subTransactionId = Integer.toUnsignedLong(buffer.getInt());
            LOGGER.trace("Sub-transaction id: {}", subTransactionId);
            return subTransactionId;
        }
        return null;
    }

    /**
     * Immediately processes the given {@link ReplicationMessage} or buffers it depending on the current streaming state.
     * <p>
     * If streaming is active and the system is not currently searching for a WAL (Write-Ahead Log) position,
     * the message is handled as follows:
     * <ul>
     *     <li>If it is a begin message, it starts buffering the transaction messages.</li>
     *     <li>If it is a commit message, it commits the buffered messages for the transaction.</li>
     *     <li>Otherwise, the message is added to the buffer for the ongoing transaction.</li>
     * </ul>
     * If streaming is inactive or WAL searching is in progress, the message is processed immediately via the provided processor.
     * </p>
     *
     * @param replicationMessage the replication message to process or buffer; must not be null.
     * @param processor          the processor responsible for handling immediate processing; must not be null.
     * @throws SQLException           if a database access error occurs during processing.
     * @throws InterruptedException   if the processing thread is interrupted.
     */
    private void immediatelyProcessMessageOrBuffer(ReplicationMessage replicationMessage, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {
        if (isStreaming && !isSearchingWAL()) {
            if (isBeginMessage) {
                this.bufferTransactions.beginMessage(super.getTransactionId(), subTransactionId, replicationMessage);
            }
            else if (isCommitMessage) {
                long subTransactionId = this.subTransactionId != null ? this.subTransactionId : -1;
                this.bufferTransactions.commitMessage(super.getTransactionId(), subTransactionId, replicationMessage);
            }
            else {
                this.bufferTransactions.addToBuffer(super.getTransactionId(), subTransactionId, replicationMessage);
            }
        }
        else {
            processor.process(replicationMessage, super.getTransactionId());
        }
    }

    private void resetValuesForNextStream() {
        LOGGER.trace("Resetting variables 'transaction id', 'sub-transaction id', 'isStreaming', 'isStreaming' and 'isStreaming' to null.");
        setTransactionId(null);
        subTransactionId = null;
        isStreaming = false;
        isBeginMessage = false;
        isCommitMessage = false;
    }
}
