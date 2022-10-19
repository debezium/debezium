/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.InsertHeader;
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

        properties.setProperty("transforms.a.type", InsertHeader.class.getName());
        properties.setProperty("transforms.a.header", "h1");
        properties.setProperty("transforms.a.value.literal", "a");
        properties.setProperty("transforms.a.predicate", "hasheader");

        properties.setProperty("transforms.b.type", InsertHeader.class.getName());
        properties.setProperty("transforms.b.header", "h1");
        properties.setProperty("transforms.b.value.literal", "b");
        properties.setProperty("transforms.b.predicate", "hasheader");
        properties.setProperty("transforms.b.negate", "true");

        Configuration configuration = Configuration.from(properties);
        SourceRecord updated;

        try (Transformations transformations = new Transformations(configuration)) {

            Transformation<SourceRecord> a = transformations.getTransformation("a");
            assertNotNull(a);
            updated = a.apply(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t1", 1, null, "key", null, "value", System.currentTimeMillis(),
                    new ConnectHeaders().addString("existingHeader", "someValue")));
            assertNotNull(updated.headers().lastWithName("h1"));
            assertEquals("a", updated.headers().lastWithName("h1").value());

            // record does not have header, transformation not applied
            a = transformations.getTransformation("a");
            updated = a.apply(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t1", 1, null, "key", null, "value", System.currentTimeMillis(),
                    new ConnectHeaders()));
            assertNull(updated.headers().lastWithName("h1"));

            // record does not have header, but transformation will still apply due to negation
            Transformation<SourceRecord> b = transformations.getTransformation("b");
            updated = b.apply(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "t1", 1, null, "key", null, "value", System.currentTimeMillis(),
                    new ConnectHeaders()));

            assertNotNull(updated.headers().lastWithName("h1"));
            assertEquals("b", updated.headers().lastWithName("h1").value());
        }
    }
}
