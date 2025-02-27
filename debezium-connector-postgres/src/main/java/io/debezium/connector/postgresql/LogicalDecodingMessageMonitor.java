/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.postgresql.connection.LogicalDecodingMessage;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;
import io.debezium.util.HexConverter;

/**
 * The class receives {@link LogicalDecodingMessage} events and delivers the event to the dedicated topic.
 * <p>
 * Every {@code MESSAGE} event has its {@code payload} block enriched to contain
 *
 * <ul>
 * <li> boolean that signifies if the message is transactional </li>
 * <li> message prefix </li>
 * <li> message content that is converted based on the connector's configured {@code binary.handling.mode}</li>
 * </ul>
 *
 * @author Lairen Hightower
 */
public class LogicalDecodingMessageMonitor {

    public static final String LOGICAL_DECODING_MESSAGE_TOPIC_SUFFIX = ".message";
    public static final String DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY = "message";
    public static final String DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY = "prefix";
    public static final String DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY = "content";

    private final SchemaNameAdjuster schemaNameAdjuster;
    private final BlockingConsumer<SourceRecord> sender;
    private final String topicName;
    private final BinaryHandlingMode binaryMode;
    private final Encoder base64Encoder;
    private final Encoder base64UrlSafeEncoder;

    /**
     * The key schema; a struct like this:
     * <p>
     * {@code
     * { "prefix" : "my-prefix" }
     * }
     * <p>
     * Using a struct over the plain prefix as a string for better evolvability down the road.
     */
    private final Schema keySchema;
    private final Schema blockSchema;
    private final Schema valueSchema;

    public LogicalDecodingMessageMonitor(PostgresConnectorConfig connectorConfig, BlockingConsumer<SourceRecord> sender) {
        this.schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        this.sender = sender;
        this.topicName = connectorConfig.getLogicalName() + LOGICAL_DECODING_MESSAGE_TOPIC_SUFFIX;
        this.binaryMode = connectorConfig.binaryHandlingMode();
        this.base64Encoder = Base64.getEncoder();
        this.base64UrlSafeEncoder = Base64.getUrlEncoder();

        this.keySchema = PostgresSchemaFactory.get().logicalDecodingMessageMonitorKeySchema(schemaNameAdjuster);

        // pg_logical_emit_message accepts null for prefix and content, but these
        // messages are not received actually via logical decoding still marking these
        // schemas as optional, just in case we will receive null values for either
        // field at some point
        this.blockSchema = PostgresSchemaFactory.get().logicalDecodingMessageMonitorBlockSchema(schemaNameAdjuster, binaryMode);

        this.valueSchema = PostgresSchemaFactory.get().logicalDecodingMessageMonitorValueSchema(schemaNameAdjuster, connectorConfig, binaryMode);
    }

    public void logicalDecodingMessageEvent(Partition partition, OffsetContext offsetContext, Long timestamp,
                                            LogicalDecodingMessage message, TransactionMonitor transactionMonitor)
            throws InterruptedException {
        final Struct logicalMsgStruct = new Struct(blockSchema);
        logicalMsgStruct.put(DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY, message.getPrefix());
        logicalMsgStruct.put(DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY, convertContent(message.getContent()));

        Struct key = new Struct(keySchema);
        key.put(DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY, message.getPrefix());

        final Struct value = new Struct(valueSchema);
        value.put(Envelope.FieldName.OPERATION, Envelope.Operation.MESSAGE.code());
        value.put(Envelope.FieldName.TIMESTAMP, timestamp);
        value.put(DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY, logicalMsgStruct);
        value.put(Envelope.FieldName.SOURCE, offsetContext.getSourceInfo());

        transactionMonitor.dataEvent(partition, new LogicalDecodingMessageId(), offsetContext, key, value);

        sender.accept(new SourceRecord(partition.getSourcePartition(), offsetContext.getOffset(), topicName,
                keySchema, key, value.schema(), value));

        if (message.isLastEventForLsn()) {
            offsetContext.getTransactionContext().endTransaction();
        }
    }

    private Object convertContent(byte[] content) {
        switch (binaryMode) {
            case BASE64:
                return new String(base64Encoder.encode(content), StandardCharsets.UTF_8);
            case BASE64_URL_SAFE:
                return new String(base64UrlSafeEncoder.encode(content), StandardCharsets.UTF_8);
            case HEX:
                return HexConverter.convertToHexString(content);
            case BYTES:
                return ByteBuffer.wrap(content);
            default:
                return ByteBuffer.wrap(content);
        }
    }

    public class LogicalDecodingMessageId implements DataCollectionId {

        private final static String LOGICAL_DECODING_MESSAGE_ID = "LOGICAL_DECODING_MESSAGE";

        @Override
        public String identifier() {
            return LOGICAL_DECODING_MESSAGE_ID;
        }

        @Override
        public List<String> parts() {
            return Collect.arrayListOf(LOGICAL_DECODING_MESSAGE_ID);
        }

        @Override
        public List<String> databaseParts() {
            return null;
        }

        @Override
        public List<String> schemaParts() {
            return null;
        }
    }
}
