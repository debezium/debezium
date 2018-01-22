/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Before;
import org.junit.Test;

import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Json;

/**
 * Unit test for {@code UnwrapFromMongoDbEnvelope}.
 *
 * @author Sairam Polavarapu
 */
public class UnwrapFromMongoDbEnvelopeTest {

    private ExtractField<SinkRecord> afterExtractor;
    private ExtractField<SinkRecord> patchExtractor;
    private ExtractField<SinkRecord> keyExtractor;

    @Before
    public void setup() throws Exception {
        afterExtractor = new ExtractField.Value<>();
        afterExtractor.configure(Collections.singletonMap("field", "after"));
        patchExtractor = new ExtractField.Value<>();
        patchExtractor.configure(Collections.singletonMap("field", "patch"));
        keyExtractor = new ExtractField.Key<>();
        keyExtractor.configure(Collections.singletonMap("field", "id"));
    }

    @Test
    public void shouldCreateCorrectStructsFromInsertJson() {
        final Schema keySchema = SchemaBuilder.struct()
                 .field("id", Schema.STRING_SCHEMA)
                 .build();

        final Struct key = new Struct(keySchema).put("id", "{ \"$oid\" : \"5a01e6d384d7be31bf48dac7\"}");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(FieldName.AFTER, Json.builder().optional().build())
                .field("patch", Json.builder().optional().build())
                .build();

        final Struct value = new Struct(valueSchema).put("after", "{\"_id\" : {\"$oid\" : \"5a01e6d384d7be31bf48dac7\"},\"borough\" : \"Manhattan\",\"cuisine\" : \"Irish\"}")
                .put("patch", null);

        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, valueSchema, value, 0);

        final SinkRecord afterRecord = afterExtractor.apply(record);
        BsonDocument valueRecord = BsonDocument.parse(afterRecord.value().toString());
        BsonDocument doc = new BsonDocument().append("id", valueRecord.get("_id"));
        BsonDocument keyRecord = BsonDocument.parse(doc.toString());

        SchemaBuilder valueBuilder = SchemaBuilder.struct();
        SchemaBuilder keyBuilder = SchemaBuilder.struct();

        for (Entry<String, BsonValue> entry : valueRecord.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, valueBuilder);
        }

        for (Entry<String, BsonValue> entry : keyRecord.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, valueBuilder);
        }

        Schema finalValueSchema = valueBuilder.build();
        Schema finalKeySchema = keyBuilder.build();
        Struct valueStruct = new Struct(finalValueSchema);
        Struct keyStruct = new Struct(finalValueSchema);

        for (Entry<String, BsonValue> entry : valueRecord.entrySet()) {
            MongoDataConverter.convertRecord(entry, finalValueSchema, valueStruct);
        }

        for (Entry<String, BsonValue> entry : keyRecord.entrySet()) {
            MongoDataConverter.convertRecord(entry, finalKeySchema, keyStruct);
        }

        assertThat(valueStruct.toString()).isEqualTo(
                "Struct{"
                + "_id=5a01e6d384d7be31bf48dac7,"
                + "borough=Manhattan,"
                + "cuisine=Irish"
              + "}"
        );

        assertThat(keyStruct.toString()).isEqualTo(
                "Struct{"
                + "id=5a01e6d384d7be31bf48dac7"
              + "}"
        );
    }

    @Test
    public void shouldCreateCorrectUpdateStructsFromUpdateJson() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "{ \"$oid\" : \"5a01e6d384d7be31bf48dac7\"}");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(FieldName.AFTER, Json.builder().optional().build())
                .field("patch", Json.builder().optional().build())
                .build();

        final Struct value = new Struct(valueSchema).put("after", null)
                .put("patch", "{\"$set\" : {\"cuisine\" : \"French\"}}");

        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, valueSchema, value, 0);

        final SinkRecord afterRecord = afterExtractor.apply(record);

        if (afterRecord.value() == null) {
            final SinkRecord patchRecord = patchExtractor.apply(record);
            final SinkRecord Key = keyExtractor.apply(record);
            BsonDocument valueRecord = BsonDocument.parse(patchRecord.value().toString());
            valueRecord = valueRecord.getDocument("$set");

            BsonDocument keyRecord = BsonDocument.parse("{ \"id\" : " + Key.key().toString() + "}");

            if (!valueRecord.containsKey("_id")) {
                valueRecord.append("_id", keyRecord.get("id"));
            }

            SchemaBuilder valueBuilder = SchemaBuilder.struct();
            SchemaBuilder keyBuilder = SchemaBuilder.struct();

            for (Entry<String, BsonValue> entry : valueRecord.entrySet()) {
                MongoDataConverter.addFieldSchema(entry, valueBuilder);
            }

            for (Entry<String, BsonValue> entry : keyRecord.entrySet()) {
                MongoDataConverter.addFieldSchema(entry, valueBuilder);
            }

            Schema finalValueSchema = valueBuilder.build();
            Schema finalKeySchema = keyBuilder.build();
            Struct finalValueStruct = new Struct(finalValueSchema);
            Struct finalKeyStruct = new Struct(finalValueSchema);

            for (Entry<String, BsonValue> entry : valueRecord.entrySet()) {
                MongoDataConverter.convertRecord(entry, finalValueSchema, finalValueStruct);
            }

            for (Entry<String, BsonValue> entry : keyRecord.entrySet()) {
                MongoDataConverter.convertRecord(entry, finalKeySchema, finalKeyStruct);
            }

            assertThat(finalValueStruct.toString()).isEqualTo(
                    "Struct{"
                    + "cuisine=French,"
                    + "_id=5a01e6d384d7be31bf48dac7"
                  + "}"
            );
            assertThat(finalKeyStruct.toString()).isEqualTo(
                    "Struct{"
                    + "id=5a01e6d384d7be31bf48dac7"
                  + "}"
            );
        }
    }
}
