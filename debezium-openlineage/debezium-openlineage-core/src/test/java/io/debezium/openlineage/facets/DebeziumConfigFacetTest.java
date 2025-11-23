/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.facets;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class DebeziumConfigFacetTest {

    @Test
    public void testSensitiveValuesAreMaskedByDefault() {
        Map<String, String> config = Map.of(
                "database.hostname", "localhost",
                "database.password", "secret123");

        DebeziumConfigFacet facet = new DebeziumConfigFacet(URI.create("http://test"), config);
        List<String> configs = facet.getConfigs();

        assertTrue(configs.stream().anyMatch(c -> c.equals("database.password=********")));
        assertTrue(configs.stream().anyMatch(c -> c.equals("database.hostname=localhost")));
    }

    @Test
    public void testCustomPatternMaskAdditionalFields() {
        Map<String, String> config = Map.of(
                "database.hostname", "localhost",
                "database.password", "secret123",
                "auth.token", "xyz789",
                "openlineage.integration.sanitize.pattern", ".*token$");

        DebeziumConfigFacet facet = new DebeziumConfigFacet(URI.create("http://test"), config);
        List<String> configs = facet.getConfigs();

        assertTrue(configs.stream().anyMatch(c -> c.equals("database.password=********")));
        assertTrue(configs.stream().anyMatch(c -> c.equals("auth.token=********")));
        assertTrue(configs.stream().anyMatch(c -> c.equals("database.hostname=localhost")));
    }

}
