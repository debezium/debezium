/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * @author Jeremy Ford
 */
public class TransformationsTest {

    @Test
    public void test() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("predicates", "hasheader");

        properties.setProperty("predicates.hasheader.type", HasHeaderKey.class.getName());
        properties.setProperty("predicates.hasheader.name", "existingHeader");

        properties.setProperty("transforms", "a,b");

        properties.setProperty("transforms.a.type", ReplaceField.class.getName() + "$Key");
        properties.setProperty("transforms.a.renames", "key:who");
        properties.setProperty("transforms.a.predicate", "hasheader");

        properties.setProperty("transforms.b.type", ReplaceField.class.getName() + "$Key");
        properties.setProperty("transforms.b.renames", "key:pro");
        properties.setProperty("transforms.b.predicate", "hasheader");
        properties.setProperty("transforms.b.negate", "true");

        Configuration configuration = Configuration.from(properties);
        SourceRecord updated;

        final Schema inputSchema = SchemaBuilder.struct()
                .field("key", Schema.STRING_SCHEMA)
                .build();

        final Struct inputStruct = new Struct(inputSchema)
                .put("key", "b2");

        final Schema expectedSchemaA = SchemaBuilder.struct()
                .field("who", Schema.STRING_SCHEMA)
                .build();

        final Schema expectedSchemaB = SchemaBuilder.struct()
                .field("pro", Schema.STRING_SCHEMA)
                .build();

        try (Transformations transformations = new Transformations(configuration)) {

            Transformation<SourceRecord> a = transformations.getTransformation("a");
            assertNotNull(a);
            updated = a.apply(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t1", 1, inputSchema, inputStruct, inputSchema, inputStruct,
                    System.currentTimeMillis(),
                    new ConnectHeaders().addString("existingHeader", "someValue")));
            assertEquals(new Struct(expectedSchemaA).put("who", "b2"), updated.key());

            // record does not have header, transformation not applied
            a = transformations.getTransformation("a");
            updated = a.apply(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t1", 1, inputSchema, inputStruct, inputSchema, inputStruct,
                    System.currentTimeMillis(),
                    new ConnectHeaders()));
            assertEquals(inputStruct, updated.key());

            // record does not have header, but transformation will still apply due to negation
            Transformation<SourceRecord> b = transformations.getTransformation("b");
            updated = b.apply(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t1", 1, inputSchema, inputStruct, inputSchema, inputStruct,
                    System.currentTimeMillis(),
                    new ConnectHeaders()));

            assertEquals(new Struct(expectedSchemaB).put("pro", "b2"), updated.key());
        }
    }
}
