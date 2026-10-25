/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonRegularExpression;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.confluent.connect.avro.AvroData;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@code MongoDataConverter} that tests Avro serialization.
 *
 * @author Jiri Pechanec
 */
public class ToAvroMongoDataConverterTest {

    private String record;
    private BsonDocument val;
    private SchemaBuilder builder;
    private AvroData avroData;
    private MongoDataConverter converter;

    @BeforeEach
    void setup() throws Exception {
        record = getFile("complex_objects.json");
        val = BsonDocument.parse(record);
        builder = SchemaBuilder.struct().name("complex");
        avroData = new AvroData(100);
        converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    }

    @Test
    void shouldCreateStructWithNestedObject() {
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();
        final Struct struct = new Struct(finalSchema);
        for (Map.Entry<String, BsonValue> bsonValueEntry : val.entrySet()) {
            converter.buildStruct(bsonValueEntry, finalSchema, struct);
        }

        final GenericData.Record avro = (GenericData.Record) avroData.fromConnectData(finalSchema, struct);
        assertThat(avro.toString()).isEqualTo(
                "{\"_id\": 1, " +
                        "\"s1\": {\"s1f1\": \"field1s1\", \"s1f2\": \"field2s1\"}, " +
                        "\"s2\": {\"s2f1\": \"field1s2\", \"s2f2\": {\"in1\": 1}}}");
    }

    @Test
    @FixFor("DBZ-650")
    public void shouldCreateSchemaWithNestedObject() {
        Map<String, Map<Object, BsonType>> entry = converter.parseBsonDocument(val);
        converter.buildSchema(entry, builder);

        final Schema finalSchema = builder.build();

        final org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(finalSchema);
        assertThat(avroSchema.toString()).isEqualTo(
                "{\"type\":\"record\",\"name\":\"complex\",\"fields\":[" +
                        "{\"name\":\"_id\",\"type\":[\"null\",\"int\"],\"default\":null}," +
                        "{\"name\":\"s1\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"s1\",\"namespace\":\"complex\",\"fields\":[" +
                        "{\"name\":\"s1f1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
                        "{\"name\":\"s1f2\",\"type\":[\"null\",\"string\"],\"default\":null}]," +
                        "\"connect.name\":\"complex.s1\"}],\"default\":null}," +
                        "{\"name\":\"s2\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"s2\",\"namespace\":\"complex\",\"fields\":[" +
                        "{\"name\":\"s2f1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
                        "{\"name\":\"s2f2\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"s2f2\",\"namespace\":\"complex.s2\",\"fields\":[" +
                        "{\"name\":\"in1\",\"type\":[\"null\",\"int\"],\"default\":null}]," +
                        "\"connect.name\":\"complex.s2.s2f2\"}],\"default\":null}]," +
                        "\"connect.name\":\"complex.s2\"}],\"default\":null}]," +
                        "\"connect.name\":\"complex\"}");
    }

    @ParameterizedTest
    @EnumSource(ArrayEncoding.class)
    @FixFor("debezium/dbz#2562")
    void shouldRoundTripScalarAndNestedRegularExpressions(ArrayEncoding encoding) throws IOException {
        final var first = new BsonRegularExpression("^foo", "i");
        final var second = new BsonRegularExpression("", "");
        final var input = new BsonDocument("pattern", first)
                .append("patterns", new BsonArray(List.of(new BsonArray(List.of(first, second)))));
        final var converter = new MongoDataConverter(encoding);
        converter.buildSchema(converter.parseBsonDocument(input), builder);
        final var schema = builder.build();
        final var result = new Struct(schema);
        for (final Map.Entry<String, BsonValue> entry : input.entrySet()) {
            converter.buildStruct(entry, schema, result);
        }

        final var avroSchema = avroData.fromConnectSchema(schema);
        final var avroValue = avroData.fromConnectData(schema, result);
        final var bytes = new ByteArrayOutputStream();
        final var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        new GenericDatumWriter<>(avroSchema).write(avroValue, encoder);
        encoder.flush();
        final var decoded = new GenericDatumReader<>(avroSchema).read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
        final var restored = (Struct) avroData.toConnectData(avroSchema, decoded).value();

        assertThat(restored).isEqualTo(result);
        assertThat(restored.getStruct("pattern").getString("regex")).isEqualTo("^foo");
        assertThat(restored.getStruct("pattern").getString("options")).isEqualTo("i");
        final Struct firstPattern;
        final Struct secondPattern;
        if (encoding == ArrayEncoding.ARRAY) {
            final List<List<Struct>> groups = restored.getArray("patterns");
            assertThat(groups).hasSize(1);
            assertThat(groups.get(0)).hasSize(2);
            firstPattern = groups.get(0).get(0);
            secondPattern = groups.get(0).get(1);
        }
        else {
            final var groups = restored.getStruct("patterns");
            assertThat(groups.schema().fields()).extracting("name").containsExactly("_0");
            final var group = groups.getStruct("_0");
            assertThat(group.schema().fields()).extracting("name").containsExactly("_0", "_1");
            firstPattern = group.getStruct("_0");
            secondPattern = group.getStruct("_1");
        }
        assertThat(firstPattern).isNotNull();
        assertThat(firstPattern.getString("regex")).isEqualTo("^foo");
        assertThat(firstPattern.getString("options")).isEqualTo("i");
        assertThat(secondPattern).isNotNull();
        assertThat(secondPattern.getString("regex")).isEmpty();
        assertThat(secondPattern.getString("options")).isEmpty();
    }

    private String getFile(String fileName) throws IOException, URISyntaxException {
        URL jsonResource = getClass().getClassLoader().getResource(fileName);
        return new String(
                Files.readAllBytes(Paths.get(jsonResource.toURI())),
                StandardCharsets.UTF_8);
    }
}
