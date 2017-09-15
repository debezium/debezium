/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.MessageDecoder;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;

/**
 * JSON deserialization of a message sent by
 * <a href="https://github.com/eulerto/wal2json">wal2json</a> logical decoding plugin. The plugin sends all
 * changes in one transaction as a single batch.
 * 
 * @author Jiri Pechanec
 *
 */
public class WAL2JSONMessageDecoder implements MessageDecoder {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final DateTimeFormat dateTime = DateTimeFormat.get();
    public WAL2JSONMessageDecoder() {
        super();
    }

    @Override
    public List<ReplicationMessage> deserializeMessage(final ByteBuffer buffer) {
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            byte[] source = buffer.array();
            byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length);
            final Document message = DocumentReader.defaultReader().read(content);
            logger.debug("Message arrived for decoding {}", message);
            final int txId = message.getInteger("xid");
            final String timestamp = message.getString("timestamp");
            final long commitTime = dateTime.systemTimestamp(timestamp);
            final Array changes = message.getArray("change");
            return changes.streamValues()
                .map(x -> new WAL2JSONReplicationMessage(txId, commitTime, x.asDocument()))
                .collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
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
