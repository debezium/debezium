/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@code MongoDataConverter}.
 *
 * @author Sairam Polavarapu
 */
public class MongoDataConverterTest {

    private String record;
    private BsonDocument val;
    private SchemaBuilder builder;

    @Before
    public void setup() throws Exception {
        record = getFile("restaurants5.json");
        val = BsonDocument.parse(record);
        builder = SchemaBuilder.struct();
    }

    @Test
    public void shouldCreateCorrectStructFromInsertJson() {
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }

        Schema finalSchema = builder.build();
        Struct struct = new Struct(finalSchema);

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.convertRecord(entry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{"
                + "address=Struct{"
                  + "building=1007,"
                  + "coord=[-73.856077, 40.848447],"
                  + "street=Morris Park Ave,"
                  + "zipcode=10462"
                + "},"
                + "borough=Bronx,"
                + "cuisine=Bakery,"
                + "grades=["
                  + "Struct{date=1393804800000,grade=A,score=2}, "
                  + "Struct{date=1378857600000,grade=A,score=6}, "
                  + "Struct{date=1358985600000,grade=A,score=10}, "
                  + "Struct{date=1322006400000,grade=A,score=9}, "
                  + "Struct{date=1299715200000,grade=B,score=14}"
                + "],"
                + "name=Morris Park Bake Shop,"
                + "restaurant_id=30075445"
              + "}"
        );
    }

    @Test
    public void shouldCreateCorrectSchemaFromInsertJson() {
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            MongoDataConverter.addFieldSchema(entry, builder);
        }
        Schema finalSchema = builder.build();

        assertThat(finalSchema).isEqualTo(
                SchemaBuilder.struct()
                    .field("address", SchemaBuilder.struct()
                            .field("building", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("coord", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).build())
                            .field("street", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("zipcode", Schema.OPTIONAL_STRING_SCHEMA).build())
                    .field("borough", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("cuisine", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("grades", SchemaBuilder.array(SchemaBuilder.struct()
                            .field("date", Schema.OPTIONAL_INT64_SCHEMA)
                            .field("grade", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("score", Schema.OPTIONAL_INT32_SCHEMA).build())
                            .build()
                    )
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("restaurant_id", Schema.OPTIONAL_STRING_SCHEMA)
                    .build()
        );
    }

    private String getFile(String fileName) throws IOException, URISyntaxException {
        URL jsonResource = getClass().getClassLoader().getResource(fileName);
        return new String(
                Files.readAllBytes(Paths.get(jsonResource.toURI())),
                StandardCharsets.UTF_8
        );
    }
}
