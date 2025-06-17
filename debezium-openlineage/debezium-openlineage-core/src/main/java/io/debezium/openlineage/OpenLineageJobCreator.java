/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;

import io.openlineage.client.OpenLineage;

public class OpenLineageJobCreator {

    private static final String PROCESSING_TYPE = "STREAMING";
    private static final String INTEGRATION = "DEBEZIUM";
    private static final String JOB_TYPE = "TASK";
    private static final String TAGS_SOURCE = "CONFIG";

    private final OpenLineageContext context;

    public OpenLineageJobCreator(OpenLineageContext context) {
        this.context = context;
    }

    public OpenLineage.Job create() {

        List<OpenLineage.TagsJobFacetFields> tags = context.getConfiguration().job().tags().entrySet().stream()
                .map(pair -> context.getOpenLineage().newTagsJobFacetFields(pair.getKey(), pair.getValue(), TAGS_SOURCE))
                .toList();

        List<OpenLineage.OwnershipJobFacetOwners> owners = context.getConfiguration().job().owners().entrySet().stream()
                .map(pair -> context.getOpenLineage().newOwnershipJobFacetOwners(pair.getKey(), pair.getValue()))
                .toList();

        OpenLineage.JobFacets jobFacets = context.getOpenLineage().newJobFacetsBuilder()
                .documentation(
                        context.getOpenLineage().newDocumentationJobFacet(
                                context.getConfiguration().job().description()))
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
