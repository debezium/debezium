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

import io.debezium.connector.mongodb.transforms.UnwrapFromMongoDbEnvelope.ArrayEncoding;
import io.debezium.doc.FixFor;

/**
 * Unit test for {@code MongoDataConverter}.
 *
 * @author Sairam Polavarapu
 */
public class MongoDataConverterTest {

    private String record;
    private BsonDocument val;
    private SchemaBuilder builder;
    private MongoDataConverter converter;

    @Before
    public void setup() throws Exception {
        record = getFile("restaurants5.json");
        val = BsonDocument.parse(record);
        builder = SchemaBuilder.struct().name("pub");
        converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    }

    @Test
    public void shouldCreateCorrectStructFromInsertJson() {
        for (Entry<String, BsonValue> entry : val.entrySet()) {
            converter.addFieldSchema(entry, builder);
        }

        Schema finalSchema = builder.build();
        Struct struct = new Struct(finalSchema);

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            converter.convertRecord(entry, finalSchema, struct);
        }

        assertThat(struct.toString()).isEqualTo(
                "Struct{"
                + "address=Struct{"
                  + "building=1007,"
                  + "floor=Struct{"
                    + "level=17,"
                    + "description=level 17"
                  + "},"
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
            converter.addFieldSchema(entry, builder);
        }
        Schema finalSchema = builder.build();

        assertThat(finalSchema).isEqualTo(
                SchemaBuilder.struct().name("pub")
                    .field("address", SchemaBuilder.struct().name("pub.address").optional()
                            .field("building", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("floor", SchemaBuilder.struct().name("pub.address.floor").optional()
                                    .field("level", Schema.OPTIONAL_INT32_SCHEMA)
                                    .field("description", Schema.OPTIONAL_STRING_SCHEMA)
                                    .build()
                            )
                            .field("coord", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
                            .field("street", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("zipcode", Schema.OPTIONAL_STRING_SCHEMA)
                            .build()
                    )
                    .field("borough", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("cuisine", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("grades", SchemaBuilder.array(SchemaBuilder.struct().name("pub.grades").optional()
                            .field("date", Schema.OPTIONAL_INT64_SCHEMA)
                            .field("grade", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("score", Schema.OPTIONAL_INT32_SCHEMA)
                            .build())
                        .optional()
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

    @Test
    @FixFor("DBZ-928")
    public void shouldProcessNullValue() {
        val = BsonDocument.parse("{\n" +
                "    \"_id\" : ObjectId(\"51e5619ee4b01f9fbdfba9fc\"),\n"+ 
                "    \"delivery\" : {\n" +
                "        \"hour\" : null,\n" +
                "        \"hourId\" : 10\n" +
                "    }\n" +
                "}");
        builder = SchemaBuilder.struct().name("withnull");
        converter = new MongoDataConverter(ArrayEncoding.ARRAY);

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            converter.addFieldSchema(entry, builder);
        }
        Schema finalSchema = builder.build();
        Struct struct = new Struct(finalSchema);

        for (Entry<String, BsonValue> entry : val.entrySet()) {
            converter.convertRecord(entry, finalSchema, struct);
        }

        assertThat(finalSchema).isEqualTo(
            SchemaBuilder.struct().name("withnull")
                .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("delivery", SchemaBuilder.struct().name("withnull.delivery").optional()
                        .field("hour", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("hourId", Schema.OPTIONAL_INT32_SCHEMA)
                        .build()
                )
                .build()
        );
        assertThat(struct.toString()).isEqualTo(
                "Struct{"
                + "_id=51e5619ee4b01f9fbdfba9fc,"
                  + "delivery=Struct{"
                    + "hourId=10"
                  + "}"
              + "}"
        );
    }
}
