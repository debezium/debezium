/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Unit tests for {@link TiDbSchema}.
 *
 * @author Aviral Srivastava
 */
public class TiDbSchemaTest {

    private static final TableId TABLE_ID = new TableId("inventory", null, "products");

    private TiDbSchema schema;

    @BeforeEach
    public void setUp() {
        final TiDbConnectorConfig connectorConfig = new TiDbConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory")
                .build());
        @SuppressWarnings("unchecked")
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(TiDbConnectorConfig.TOPIC_NAMING_STRATEGY);
        schema = new TiDbSchema(topicNamingStrategy, connectorConfig.getSourceInfoStructMaker().schema(),
                connectorConfig.schemaNameAdjuster());
    }

    private static Schema rowSchema(boolean withDescription) {
        final SchemaBuilder builder = SchemaBuilder.struct().optional()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA);
        if (withDescription) {
            builder.field("description", Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder.build();
    }

    private static Schema keySchema() {
        return SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
    }

    @Test
    public void shouldKnowNoTablesInitially() {
        assertThat(schema.schemaFor(TABLE_ID)).isNull();
        assertThat(schema.tableIds()).isEmpty();
    }

    @Test
    public void shouldBuildEnvelopeFromRefreshedRowSchema() {
        final TiDbTableSchema tableSchema = schema.refresh(TABLE_ID, keySchema(), rowSchema(false));

        assertThat(schema.schemaFor(TABLE_ID)).isSameAs(tableSchema);
        assertThat(tableSchema.id()).isEqualTo(TABLE_ID);
        assertThat(tableSchema.keySchema().field("id")).isNotNull();

        final Schema valueSchema = tableSchema.getEnvelopeSchema().schema();
        assertThat(valueSchema.name()).isEqualTo("tidb_server.inventory.products.Envelope");
        assertThat(valueSchema.field("before").schema().field("name")).isNotNull();
        assertThat(valueSchema.field("after").schema().field("name")).isNotNull();
        assertThat(valueSchema.field("source").schema().field(SourceInfo.COMMIT_TS_KEY)).isNotNull();
    }

    @Test
    public void shouldReuseSchemaWhenUnchanged() {
        final TiDbTableSchema first = schema.refresh(TABLE_ID, keySchema(), rowSchema(false));
        final TiDbTableSchema second = schema.refresh(TABLE_ID, keySchema(), rowSchema(false));
        assertThat(second).isSameAs(first);
    }

    @Test
    public void shouldRebuildSchemaWhenRowSchemaChanges() {
        final TiDbTableSchema first = schema.refresh(TABLE_ID, keySchema(), rowSchema(false));
        final TiDbTableSchema second = schema.refresh(TABLE_ID, keySchema(), rowSchema(true));

        assertThat(second).isNotSameAs(first);
        assertThat(second.getEnvelopeSchema().schema().field("after").schema().field("description")).isNotNull();
    }
}
