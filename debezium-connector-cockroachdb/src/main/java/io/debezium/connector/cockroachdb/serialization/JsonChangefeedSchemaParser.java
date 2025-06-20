/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb.serialization;

import java.util.Map;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser.ParsedChange;

/**
 * Parses CockroachDB changefeed messages formatted as JSON into Debezium-compatible
 * key and value schema/data using Kafka Connect's JsonConverter.
 * <p>
 * This parser expects the input strings to be raw JSON objects emitted via
 * `envelope=enriched` changefeeds from CockroachDB.
 *
 * @author Virag Tripathi
 */
public class JsonChangefeedSchemaParser implements ChangefeedSchemaParser {

    private final JsonConverter converter;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonChangefeedSchemaParser() {
        this.converter = new JsonConverter();
        this.converter.configure(Map.of("schemas.enable", "false"), false);
    }

    @Override
    public ParsedChange parse(String keyJson, String valueJson) throws Exception {
        SchemaAndValue key = converter.toConnectData("dummy", keyJson.getBytes());
        SchemaAndValue value = converter.toConnectData("dummy", valueJson.getBytes());

        return new ParsedChange(key.schema(), key.value(), value.schema(), value.value());
    }
}
