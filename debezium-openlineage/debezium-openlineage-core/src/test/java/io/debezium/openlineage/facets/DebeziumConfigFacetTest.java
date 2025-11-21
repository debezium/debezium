package io.debezium.openlineage.facets;

import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class DebeziumConfigFacetTest {

    @Test
    public void testSensitiveValuesAreMaskedByDefault() {
        Map<String, String> config = Map.of(
                "database.hostname", "localhost",
                "database.password", "secret123"
        );

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
                "openlineage.integration.sanitize.pattern", ".*token$"
        );

        DebeziumConfigFacet facet = new DebeziumConfigFacet(URI.create("http://test"), config);
        List<String> configs = facet.getConfigs();

        assertTrue(configs.stream().anyMatch(c -> c.equals("database.password=********")));
        assertTrue(configs.stream().anyMatch(c -> c.equals("auth.token=********")));
        assertTrue(configs.stream().anyMatch(c -> c.equals("database.hostname=localhost")));
    }

}
