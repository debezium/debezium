/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.integration.AbstractOpenLineageJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.openlineage.facets.DebeziumConfigFacet;
import io.openlineage.client.OpenLineage;

@Tag("all")
@Tag("it")
@Tag("it-postgresql")
@Tag("openlineage")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class OpenLineageJdbcSinkTest extends AbstractOpenLineageJdbcSinkTest {

    public OpenLineageJdbcSinkTest(Sink sink) {
        super(sink);
    }

    public void assertEventContainsExpectedData(OpenLineage.RunEvent startEvent) {

        assertThat(startEvent.getJob().getNamespace()).isEqualTo("my-sink-with-lineage");
        assertThat(startEvent.getJob().getName()).isEqualTo("my-sink-with-lineage.0");
        assertThat(startEvent.getJob().getFacets().getDocumentation().getDescription()).isEqualTo("This connector does cdc for products");

        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("Debezium");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getVersion()).matches("^\\d+\\.\\d+\\.\\d+.*$");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()).matches("^\\d+\\.\\d+\\.\\d+$");

        DebeziumConfigFacet debeziumConfigFacet = (DebeziumConfigFacet) startEvent.getRun().getFacets().getAdditionalProperties().get("debezium_config");

        assertThat(debeziumConfigFacet.getConfigs()).contains(
                "connection.username=test",
                "connection.password=test",
                "name=my-sink-with-lineage",
                "openlineage.integration.enabled=true",
                "openlineage.integration.job.description=This connector does cdc for products",
                "openlineage.integration.job.tags=env=prod,team=cdc",
                "openlineage.integration.job.owners=Mario=maintainer,John Doe=Data scientist")
                .anyMatch(config -> config.startsWith("openlineage.integration.config.file.path=") && config.contains("openlineage.yml"))
                .anyMatch(config -> config.startsWith("connection.url="));

        Map<String, String> tags = startEvent.getJob().getFacets().getTags().getTags()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.TagsJobFacetFields::getKey,
                        OpenLineage.TagsJobFacetFields::getValue));

        assertThat(startEvent.getProducer().toString()).startsWith("https://github.com/debezium/debezium/");
        assertThat(tags).contains(entry("env", "prod"), entry("team", "cdc"));

        Map<String, String> ownership = startEvent.getJob().getFacets().getOwnership().getOwners()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.OwnershipJobFacetOwners::getName,
                        OpenLineage.OwnershipJobFacetOwners::getType));

        assertThat(ownership).contains(entry("Mario", "maintainer"), entry("John Doe", "Data scientist"));
    }
}
