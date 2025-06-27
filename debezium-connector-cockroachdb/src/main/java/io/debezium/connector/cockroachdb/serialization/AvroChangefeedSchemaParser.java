/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb.serialization;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaAvroDeserializerConfig;

import io.confluent.connect.avro.AvroConverter;
import io.debezium.connector.cockroachdb.serialization.ChangefeedSchemaParser.ParsedChange;

/**
 * Parses CockroachDB changefeed messages formatted as Avro using Kafka Connect's AvroConverter.
 * Requires that the Kafka Avro schema registry is available and configured.
 *
 * @author Virag Tripathi
 */
public class AvroChangefeedSchemaParser implements ChangefeedSchemaParser {

    private final Converter avroConverter;

    public AvroChangefeedSchemaParser(String schemaRegistryUrl) {
        this.avroConverter = new AvroConverter();
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        this.avroConverter.configure(config, false);
    }

    @Override
    public ParsedChange parse(String keyBytesBase64, String valueBytesBase64) throws Exception {
        byte[] keyBytes = java.util.Base64.getDecoder().decode(keyBytesBase64);
        byte[] valueBytes = java.util.Base64.getDecoder().decode(valueBytesBase64);

        SchemaAndValue key = avroConverter.toConnectData("dummy", keyBytes);
        SchemaAndValue value = avroConverter.toConnectData("dummy", valueBytes);

        return new ParsedChange(key.schema(), key.value(), value.schema(), value.value());
    }
}
