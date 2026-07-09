/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb.ticdc;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.TableId;

/**
 * Parses the Debezium-format JSON messages produced by a TiCDC changefeed configured with
 * {@code protocol=debezium}.
 * <p>
 * The typed {@code before}/{@code after} images and the key are decoded with the standard Kafka
 * Connect {@link JsonConverter}, so the inline {@code schema} block of the message defines the
 * row schema. TiCDC fills the MySQL-specific {@code source} fields with dummy values
 * ({@code server_id=0}, {@code file=""}, {@code pos=0}) and carries the real position in
 * extension fields; those extensions ({@code commitTs}, {@code clusterId}) are read from the raw
 * JSON since they are not always declared in the message schema.
 *
 * @author Aviral Srivastava
 */
public class TiCdcEventParser implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiCdcEventParser.class);

    private static final String SCHEMA_FIELD = "schema";
    private static final String PAYLOAD_FIELD = "payload";

    private static final String[] COMMIT_TS_FIELDS = { "commitTs", "commit_ts" };
    private static final String[] CLUSTER_ID_FIELDS = { "clusterId", "cluster_id", "clusterID" };

    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonConverter keyConverter = new JsonConverter();
    private final JsonConverter valueConverter = new JsonConverter();

    public TiCdcEventParser() {
        keyConverter.configure(Map.of("schemas.enable", "true"), true);
        valueConverter.configure(Map.of("schemas.enable", "true"), false);
    }

    /**
     * Parses a single message of a TiCDC changefeed topic.
     *
     * @param topic the TiCDC topic the message was read from
     * @param key the raw message key, may be {@code null}
     * @param value the raw message value, may be {@code null} for tombstones
     * @return the parsed event, or {@code null} if the message does not represent a data change
     *         (tombstones, watermarks, DDL announcements)
     */
    public TiCdcEvent parse(String topic, byte[] key, byte[] value) {
        if (value == null) {
            LOGGER.trace("Skipping tombstone message on TiCDC topic {}", topic);
            return null;
        }

        final JsonNode message = readTree(topic, value);
        if (!message.hasNonNull(PAYLOAD_FIELD) || !message.hasNonNull(SCHEMA_FIELD)) {
            throw new DebeziumException("Message on TiCDC topic '" + topic + "' has no schema/payload envelope. "
                    + "The connector requires the TiCDC changefeed to produce Debezium-format JSON messages with inline schemas "
                    + "(sink-uri option 'protocol=debezium')");
        }

        final JsonNode payload = message.get(PAYLOAD_FIELD);
        final JsonNode opNode = payload.get("op");
        if (opNode == null || opNode.isNull()) {
            LOGGER.debug("Skipping non data change message on TiCDC topic {}", topic);
            return null;
        }
        final Operation operation = Operation.forCode(opNode.asText());
        if (operation == null || operation == Operation.TRUNCATE || operation == Operation.MESSAGE) {
            LOGGER.debug("Skipping message with unsupported operation '{}' on TiCDC topic {}", opNode.asText(), topic);
            return null;
        }

        final JsonNode source = payload.get(FieldName.SOURCE);
        if (source == null || source.isNull()) {
            LOGGER.warn("Skipping data change message without source info on TiCDC topic {}", topic);
            return null;
        }
        final String db = textOf(source, "db");
        final String table = textOf(source, "table");
        if (db == null || table == null) {
            LOGGER.warn("Skipping data change message without database/table info on TiCDC topic {}", topic);
            return null;
        }
        final TableId tableId = new TableId(db, null, table);

        final long commitTs = longOf(source, COMMIT_TS_FIELDS);
        final String clusterId = textOf(source, CLUSTER_ID_FIELDS);
        // The source-level ts_ms is the commit time in TiDB, the payload-level one is the TiCDC processing time
        long sourceTsMs = longOf(source, "ts_ms");
        if (sourceTsMs == 0) {
            sourceTsMs = longOf(payload, "ts_ms");
        }

        final SchemaAndValue typedValue = valueConverter.toConnectData(topic, value);
        if (!(typedValue.value() instanceof Struct envelope)) {
            throw new DebeziumException("Unexpected non-struct payload on TiCDC topic '" + topic + "'");
        }
        final Schema rowSchema = rowSchemaOf(typedValue.schema(), topic);
        final Struct before = structOrNull(envelope, FieldName.BEFORE);
        final Struct after = structOrNull(envelope, FieldName.AFTER);

        Struct keyStruct = null;
        Schema keySchema = null;
        if (key != null && key.length > 0) {
            final SchemaAndValue typedKey = keyConverter.toConnectData(topic, key);
            if (typedKey.value() instanceof Struct struct) {
                keyStruct = struct;
                keySchema = typedKey.schema();
            }
        }

        return new TiCdcEvent(tableId, operation, before, after, rowSchema, keyStruct, keySchema,
                Instant.ofEpochMilli(sourceTsMs), commitTs, clusterId);
    }

    private JsonNode readTree(String topic, byte[] value) {
        try {
            return mapper.readTree(value);
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to parse message on TiCDC topic '" + topic + "' as JSON", e);
        }
    }

    private static Schema rowSchemaOf(Schema envelopeSchema, String topic) {
        final org.apache.kafka.connect.data.Field afterField = envelopeSchema.field(FieldName.AFTER);
        if (afterField != null) {
            return afterField.schema();
        }
        final org.apache.kafka.connect.data.Field beforeField = envelopeSchema.field(FieldName.BEFORE);
        if (beforeField != null) {
            return beforeField.schema();
        }
        throw new DebeziumException("Message on TiCDC topic '" + topic + "' declares neither a before nor an after schema");
    }

    private static Struct structOrNull(Struct struct, String fieldName) {
        return struct.schema().field(fieldName) != null ? struct.getStruct(fieldName) : null;
    }

    private static String textOf(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            final JsonNode field = node.get(fieldName);
            if (field != null && !field.isNull()) {
                return field.asText();
            }
        }
        return null;
    }

    private static long longOf(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            final JsonNode field = node.get(fieldName);
            if (field != null && field.isNumber()) {
                return field.asLong();
            }
        }
        return 0;
    }

    @Override
    public void close() {
        keyConverter.close();
        valueConverter.close();
    }
}
