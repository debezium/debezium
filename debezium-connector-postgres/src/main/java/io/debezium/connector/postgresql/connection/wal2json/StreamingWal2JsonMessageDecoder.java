/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationMessage.NoopMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.TransactionMessage;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;

/**
 * <p>JSON deserialization of a message sent by
 * <a href="https://github.com/eulerto/wal2json">wal2json</a> logical decoding plugin. The plugin sends all
 * changes in one transaction as a single batch and they are passed to processor one-by-one.
 * The JSON file arrives in chunks of a big JSON file where the chunks are not valid JSON itself.</p>
 *
 * <p>There are four different chunks that can arrive from the decoder.
 * <b>Beginning of message</b></br>
 * <pre>
 * {
 *   "xid": 563,
 *   "timestamp": "2018-03-20 10:58:43.396355+01",
 *   "change": [
 * </pre>
 *
 * <b>First change</b>
 * <pre>
 *      {
 *          "kind": "insert",
 *          "schema": "public",
 *          "table": "numeric_decimal_table",
 *          "columnnames": ["pk", "d", "dzs", "dvs", "d_nn", "n", "nzs", "nvs", "d_int", "dzs_int", "dvs_int", "n_int", "nzs_int", "nvs_int", "d_nan", "dzs_nan", "dvs_nan", "n_nan", "nzs_nan", "nvs_nan"],
 *          "columntypes": ["integer", "numeric(3,2)", "numeric(4,0)", "numeric", "numeric(3,2)", "numeric(6,4)", "numeric(4,0)", "numeric", "numeric(3,2)", "numeric(4,0)", "numeric", "numeric(6,4)", "numeric(4,0)", "numeric", "numeric(3,2)", "numeric(4,0)", "numeric", "numeric(6,4)", "numeric(4,0)", "numeric"],
 *          "columnoptionals": [false, true, true, true, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true],
 *          "columnvalues": [1, 1.10, 10, 10.1111, 3.30, 22.2200, 22, 22.2222, 1.00, 10, 10, 22.0000, 22, 22, null, null, null, null, null, null]
 *      }
 * </pre>
 *
 * <b>Further changes</b>
 * <pre>
 *      ,{
 *          "kind": "insert",
 *          "schema": "public",
 *          "table": "numeric_decimal_table",
 *          "columnnames": ["pk", "d", "dzs", "dvs", "d_nn", "n", "nzs", "nvs", "d_int", "dzs_int", "dvs_int", "n_int", "nzs_int", "nvs_int", "d_nan", "dzs_nan", "dvs_nan", "n_nan", "nzs_nan", "nvs_nan"],
 *          "columntypes": ["integer", "numeric(3,2)", "numeric(4,0)", "numeric", "numeric(3,2)", "numeric(6,4)", "numeric(4,0)", "numeric", "numeric(3,2)", "numeric(4,0)", "numeric", "numeric(6,4)", "numeric(4,0)", "numeric", "numeric(3,2)", "numeric(4,0)", "numeric", "numeric(6,4)", "numeric(4,0)", "numeric"],
 *          "columnoptionals": [false, true, true, true, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true],
 *          "columnvalues": [1, 1.10, 10, 10.1111, 3.30, 22.2200, 22, 22.2222, 1.00, 10, 10, 22.0000, 22, 22, null, null, null, null, null, null]
 *      }
 * </pre>
 *
 * <b>End of message</b>
 * <pre>
 *      ]
 * }
 * </pre>
 * </p>
 *
 * <p>
 * For parsing purposes it is necessary to add or remove a fragment of JSON to make a well-formatted JSON out of it.
 * The last message is just dropped.
 * </p>
 * @author Jiri Pechanec
 *
 */
public class StreamingWal2JsonMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWal2JsonMessageDecoder.class);

    private static final byte TAB = 9;
    private static final byte CR = 13;
    private static final byte SPACE = 32;

    private static final byte COMMA = 44;
    private static final byte RIGHT_BRACKET = 93;
    private static final byte LEFT_BRACE = 123;
    private static final byte RIGHT_BRACE = 125;
    private static final long UNDEFINED_LONG = -1;

    private final DateTimeFormat dateTime = DateTimeFormat.get();
    private boolean containsMetadata = false;
    private boolean messageInProgress = false;

    /**
     * To identify if the last current chunk is the last one we can send the current one
     * for processing only after we read the next one or the end of message fragment.
     */
    private byte[] currentChunk;

    private long txId;

    private Instant commitTime;

    @Override
    public void processNotEmptyMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            // Extend the array by two as we might need to append two chars and set them to space by default
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length + 2);
            final int lastPos = content.length - 1;
            content[lastPos - 1] = SPACE;
            content[lastPos] = SPACE;

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Chunk arrived from database {}", new String(content));
            }

            if (!messageInProgress) {
                byte firstChar = getFirstNonWhiteChar(content);
                if (firstChar != LEFT_BRACE) {
                    outOfOrderChunk(content);
                    nonInitialChunk(processor, typeRegistry, content);
                }
                else {
                    // We received the beginning of a transaction
                    if (getLastNonWhiteChar(content) != RIGHT_BRACE) {
                        // Chunks are enabled and we have an unfinished message, it is necessary to add a sequence of closing chars
                        content[lastPos - 1] = RIGHT_BRACKET;
                        content[lastPos] = RIGHT_BRACE;
                    }
                    final Document message = DocumentReader.defaultReader().read(content);
                    if (message.has("kind")) {
                        // This is not a preamble but out-of-order change chunk
                        outOfOrderChunk(content);
                        nonInitialChunk(processor, typeRegistry, content);
                    }
                    else {
                        // Correct initial chunk
                        txId = message.getLong("xid");
                        final String timestamp = message.getString("timestamp");
                        commitTime = dateTime.systemTimestampToInstant(timestamp);
                        messageInProgress = true;
                        currentChunk = null;
                        processor.process(new TransactionMessage(Operation.BEGIN, txId, commitTime));
                    }
                }
            }
            else {
                nonInitialChunk(processor, typeRegistry, content);
            }
        }
        catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    protected void nonInitialChunk(ReplicationMessageProcessor processor, TypeRegistry typeRegistry,
                                   final byte[] content)
            throws IOException, SQLException, InterruptedException {
        byte firstChar = getFirstNonWhiteChar(content);
        // We are receiving changes in chunks
        if (firstChar == LEFT_BRACE) {
            // First change, this is a valid JSON
            currentChunk = content;
        }
        else if (firstChar == COMMA) {
            // following changes, they have an extra comma at the start of message
            if (currentChunk != null) {
                doProcessMessage(processor, typeRegistry, currentChunk, false);
            }
            replaceFirstNonWhiteChar(content, SPACE);
            currentChunk = content;
        }
        else if (firstChar == RIGHT_BRACKET) {
            // No more changes
            doProcessMessage(processor, typeRegistry, currentChunk, true);
            messageInProgress = false;
            processor.process(new TransactionMessage(Operation.COMMIT, txId, commitTime));
        }
        else {
            throw new ConnectException("Chunk arrived in unexpected state");
        }
    }

    /**
     * This method is called when a database server or Debezium crashes.
     * With wal2json streaming mode it can happen that the message preamble is no replayed
     * but the message is streamed form the middle.
     * This issue is very hard to reproduce so a precaution is taken and metadata are filled with
     * synthetic values.
     * <p>The new wal2json format will be resilient to this situation.
     *
     * @param content
     */
    protected void outOfOrderChunk(final byte[] content) {
        // This is not a standalone JSON, we are getting a transaction in progress
        // Metadata are lost, we need to create artificial ones
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("Got out of order chunk {}, recording artifical TX", new String(content));
        }
        txId = UNDEFINED_LONG;
        commitTime = Instant.now();
        messageInProgress = true;
        currentChunk = null;
    }

    private byte getLastNonWhiteChar(byte[] array) throws IllegalArgumentException {
        for (int i = array.length - 1; i >= 0; i--) {
            if (!isWhitespace(array[i])) {
                return array[i];
            }
        }
        throw new IllegalArgumentException("No non-white char");
    }

    private byte getFirstNonWhiteChar(byte[] array) throws IllegalArgumentException {
        for (int i = 0; i < array.length; i++) {
            if (!isWhitespace(array[i])) {
                return array[i];
            }
        }
        throw new IllegalArgumentException("No non-white char");
    }

    private void replaceFirstNonWhiteChar(byte[] array, byte to) {
        for (int i = 0; i < array.length; i++) {
            if (!isWhitespace(array[i])) {
                array[i] = to;
                return;
            }
        }
    }

    private boolean isWhitespace(byte c) {
        return (c >= TAB && c <= CR) || c == SPACE;
    }

    private void doProcessMessage(ReplicationMessageProcessor processor, TypeRegistry typeRegistry, byte[] content, boolean lastMessage)
            throws IOException, SQLException, InterruptedException {
        if (content != null) {
            final Document change = DocumentReader.floatNumbersAsTextReader().read(content);
            LOGGER.trace("Change arrived for decoding {}", change);
            processor.process(new Wal2JsonReplicationMessage(txId, commitTime, change, containsMetadata, lastMessage, typeRegistry));
        }
        else {
            // If content is null then this is an empty change event that WAL2JSON can generate for events like DDL,
            // truncate table, materialized views, etc. The transaction still needs to be processed for the heartbeat
            // to fire.
            LOGGER.trace("Empty change arrived");
            processor.process(new NoopMessage(txId, commitTime));
        }

    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return optionsWithoutMetadata(builder)
                .withSlotOption("include-not-null", "true");
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder
                .withSlotOption("pretty-print", 1)
                .withSlotOption("write-in-chunks", 1)
                .withSlotOption("include-xids", 1)
                .withSlotOption("include-timestamp", 1);
    }

    @Override
    public void setContainsMetadata(boolean containsMetadata) {
        this.containsMetadata = containsMetadata;
    }
}
