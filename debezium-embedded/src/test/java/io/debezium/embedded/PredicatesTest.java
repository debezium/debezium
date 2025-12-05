/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.predicates.TopicNameMatches;
import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * @author Jeremy Ford
 */
public class PredicatesTest {

    @Test
    public void test() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("predicates", "a,b");

        properties.setProperty("predicates.a.type", TopicNameMatches.class.getName());
        properties.setProperty("predicates.a.pattern", "a-.*");

        properties.setProperty("predicates.b.type", HasHeaderKey.class.getName());
        properties.setProperty("predicates.b.name", "bob");

        Configuration configuration = Configuration.from(properties);

        try (Predicates predicates = new Predicates(configuration)) {

            Predicate<SourceRecord> a = predicates.getPredicate("a");
            assertNotNull(a);
            assertTrue(a instanceof TopicNameMatches);

            Predicate<SourceRecord> b = predicates.getPredicate("b");
            assertNotNull(b);
            assertTrue(b instanceof HasHeaderKey);
        }
    }
}
