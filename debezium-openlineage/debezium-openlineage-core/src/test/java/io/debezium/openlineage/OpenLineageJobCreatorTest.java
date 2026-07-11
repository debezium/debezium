/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.openlineage.client.OpenLineage;

class OpenLineageJobCreatorTest {

    private static final URI PRODUCER = URI.create("https://github.com/debezium/debezium");

    private OpenLineage.Job createJob(Map<String, String> tags, Map<String, String> owners) {
        OpenLineage openLineage = new OpenLineage(PRODUCER);
        DebeziumOpenLineageConfiguration configuration = new DebeziumOpenLineageConfiguration(
                true,
                new DebeziumOpenLineageConfiguration.Config("openlineage.yml"),
                new DebeziumOpenLineageConfiguration.Job("namespace", "a description", tags, owners));
        OpenLineageContext context = new OpenLineageContext(
                openLineage, configuration, new OpenLineageJobIdentifier("namespace", "job"), UUID.randomUUID());

        return new OpenLineageJobCreator(context).create();
    }

    @Test
    void shouldNotAddTagsAndOwnershipFacetsWhenTagsAndOwnersAreEmpty() {
        OpenLineage.Job job = createJob(Map.of(), Map.of());

        assertNull(job.getFacets().getTags());
        assertNull(job.getFacets().getOwnership());
    }

    @Test
    void shouldAddTagsFacetWhenTagsArePresent() {
        OpenLineage.Job job = createJob(Map.of("env", "prod"), Map.of());

        assertNotNull(job.getFacets().getTags());
        assertEquals("env", job.getFacets().getTags().getTags().get(0).getKey());
        assertEquals("prod", job.getFacets().getTags().getTags().get(0).getValue());
        assertNull(job.getFacets().getOwnership());
    }

    @Test
    void shouldAddOwnershipFacetWhenOwnersArePresent() {
        OpenLineage.Job job = createJob(Map.of(), Map.of("team", "cdc"));

        assertNotNull(job.getFacets().getOwnership());
        assertEquals("team", job.getFacets().getOwnership().getOwners().get(0).getName());
        assertNull(job.getFacets().getTags());
    }

    @Test
    void shouldAlwaysAddDocumentationAndJobTypeFacets() {
        OpenLineage.Job job = createJob(Map.of(), Map.of());

        assertNotNull(job.getFacets().getDocumentation());
        assertEquals("a description", job.getFacets().getDocumentation().getDescription());
        assertNotNull(job.getFacets().getJobType());
    }
}
