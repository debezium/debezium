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

import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.document.Array;
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

    @Override
    public void processMessage(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            final Document message = DocumentReader.defaultReader().read(content);
            LOGGER.debug("Message arrived for decoding {}", message);
            final int txId = message.getInteger("xid");
            final String timestamp = message.getString("timestamp");
            final long commitTime = dateTime.systemTimestamp(timestamp);
            final Array changes = message.getArray("change");
            for (Array.Entry e: changes) {
                processor.process(new Wal2JsonReplicationMessage(txId, commitTime, e.getValue().asDocument()));
            }
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public ChainedLogicalStreamBuilder options(ChainedLogicalStreamBuilder builder) {
        return builder
            .withSlotOption("pretty-print", 1)
            .withSlotOption("write-in-chunks", 0)
            .withSlotOption("include-xids", 1)
            .withSlotOption("include-timestamp", 1);
    }
}
