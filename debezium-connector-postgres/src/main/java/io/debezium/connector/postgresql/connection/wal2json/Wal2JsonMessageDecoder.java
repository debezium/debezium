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
 * JSON deserialization of a message sent by
 * <a href="https://github.com/eulerto/wal2json">wal2json</a> logical decoding plugin. The plugin sends all
 * changes in one transaction as a single batch and they are passed to processor one-by-one.
 *
 * @author Jiri Pechanec
 *
 */
public class Wal2JsonMessageDecoder implements MessageDecoder {

    private static final  Logger LOGGER = LoggerFactory.getLogger(Wal2JsonMessageDecoder.class);

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
                    // Chunks are enabled and we have an unfinished message
                    content += "]}";
                }
                final Document message = DocumentReader.defaultReader().read(content + "]}");
                txId = message.getInteger("xid");
                timestamp = message.getString("timestamp");
                commitTime = dateTime.systemTimestamp(timestamp);
                messageInProgress = true;
                bufferedContent = null;
            }
            else {
                // We are receiving changes in chunks
                if (content.startsWith("{")) {
                    // First change
                    bufferedContent = content;
                }
                else if (content.startsWith(",")) {
                    // following changes
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
