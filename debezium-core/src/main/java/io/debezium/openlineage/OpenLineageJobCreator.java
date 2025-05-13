/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;

import io.openlineage.client.OpenLineage;

public class OpenLineageJobCreator {

    public static final String PROCESSING_TYPE = "STREAMING";
    public static final String INTEGRATION = "DEBEZIUM";
    public static final String JOB_TYPE = "TASK";
    private final OpenLineageContext context;

    public OpenLineageJobCreator(OpenLineageContext context) {
        this.context = context;
    }

    public OpenLineage.Job create() {

        // TODO Move configurations to CommonConnectorConfig
        List<OpenLineage.TagsJobFacetFields> tags = context.getConfiguration().getList("openlineage.integration.tags", ",", s -> s)
                .stream().map(pair -> pair.split("=")) // Split into key-value array
                .map(pair -> context.getOpenLineage().newTagsJobFacetFields(pair[0].trim(), pair[1].trim(), "CONFIG"))
                .toList();

        List<OpenLineage.OwnershipJobFacetOwners> owners = context.getConfiguration().getList("openlineage.integration.owners", ",", s -> s)
                .stream().map(pair -> pair.split("=")) // Split into key-value array
                .map(pair -> context.getOpenLineage().newOwnershipJobFacetOwners(pair[0].trim(), pair[1].trim()))
                .toList();

        OpenLineage.JobFacets jobFacets = context.getOpenLineage().newJobFacetsBuilder()
                // TODO put a default value
                .documentation(
                        context.getOpenLineage().newDocumentationJobFacet(
                                context.getConfiguration().getString("openlineage.integration.job.description", "")))
                .ownership(context.getOpenLineage().newOwnershipJobFacet(owners))
                .tags(context.getOpenLineage().newTagsJobFacet(tags))
                .jobType(context.getOpenLineage().newJobTypeJobFacet(PROCESSING_TYPE, INTEGRATION, JOB_TYPE))
                .build();

        return context.getOpenLineage().newJobBuilder()
                .namespace(context.getJobIdentifier().namespace())
                .name(context.getJobIdentifier().name())
                .facets(jobFacets)
                .build();
    }
}
