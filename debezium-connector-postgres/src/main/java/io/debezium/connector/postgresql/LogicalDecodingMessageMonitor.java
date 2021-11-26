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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.postgresql.connection.LogicalDecodingMessage;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.HexConverter;
import io.debezium.util.SchemaNameAdjuster;

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
        this.schemaNameAdjuster = SchemaNameAdjuster.create();
        this.sender = sender;
        this.topicName = connectorConfig.getLogicalName() + LOGICAL_DECODING_MESSAGE_TOPIC_SUFFIX;
        this.binaryMode = connectorConfig.binaryHandlingMode();
        this.base64Encoder = Base64.getEncoder();

        this.keySchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.postgresql.MessageKey"))
                .field(DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        // pg_logical_emit_message accepts null for prefix and content, but these
        // messages are not received actually via logical decoding still marking these
        // schemas as optional, just in case we will receive null values for either
        // field at some point
        this.blockSchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.postgresql.Message"))
                .field(DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY, binaryMode.getSchema().optional().build())
                .build();

        this.valueSchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.postgresql.MessageValue"))
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.SOURCE, connectorConfig.getSourceInfoStructMaker().schema())
                .field(DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY, blockSchema)
                .build();
    }

    public void logicalDecodingMessageEvent(Partition partition, OffsetContext offsetContext, Long timestamp,
                                            LogicalDecodingMessage message)
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
            case HEX:
                return HexConverter.convertToHexString(content);
            case BYTES:
                return ByteBuffer.wrap(content);
            default:
                return ByteBuffer.wrap(content);
        }
    }
}
