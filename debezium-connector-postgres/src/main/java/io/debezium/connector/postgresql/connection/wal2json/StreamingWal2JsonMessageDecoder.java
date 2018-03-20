/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
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
public class StreamingWal2JsonMessageDecoder implements MessageDecoder {

    private static final  Logger LOGGER = LoggerFactory.getLogger(StreamingWal2JsonMessageDecoder.class);

    private final DateTimeFormat dateTime = DateTimeFormat.get();
    private boolean containsMetadata = false;
    private boolean messageInProgress = false;
    private String bufferedContent;

    private int txId;

    private String timestamp;

    private long commitTime;

    @Override
    public void processMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            String content = new String(Arrays.copyOfRange(source, buffer.arrayOffset(), source.length)).trim();
            LOGGER.debug("Chunk arrived from database {}", content);

            if (!messageInProgress) {
                // We received the beginning of a transaction
                if (!content.endsWith("}")) {
                    // Chunks are enabled and we have an unfinished message, it is necessary to add a sequence of closing chars
                    content += "]}";
                }
                final Document message = DocumentReader.defaultReader().read(content);
                txId = message.getInteger("xid");
                timestamp = message.getString("timestamp");
                commitTime = dateTime.systemTimestamp(timestamp);
                messageInProgress = true;
                bufferedContent = null;
            }
            else {
                // We are receiving changes in chunks
                if (content.startsWith("{")) {
                    // First change, this is a valid JSON
                    bufferedContent = content;
                }
                else if (content.startsWith(",")) {
                    // following changes, they have an extra comma at the start of message
                    doProcessMessage(processor, typeRegistry, bufferedContent, false);
                    bufferedContent = content.substring(1);
                }
                else if (content.startsWith("]")) {
                    // No more changes
                    if (bufferedContent != null) {
                        doProcessMessage(processor, typeRegistry, bufferedContent, true);
                    }
                    messageInProgress = false;
                }
                else {
                    throw new ConnectException("Chunk arrived in unxepected state");
                }
            }
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    private void doProcessMessage(ReplicationMessageProcessor processor, TypeRegistry typeRegistry, String content, boolean lastMessage)
            throws IOException, SQLException, InterruptedException {
        final Document change = DocumentReader.floatNumbersAsTextReader().read(content);
        LOGGER.debug("Change arrived for decoding {}", change);
        processor.process(new Wal2JsonReplicationMessage(txId, commitTime, change, containsMetadata, lastMessage, typeRegistry));
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
