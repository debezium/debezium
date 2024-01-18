/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

public class TimescaleDbTest extends AbstractConnectorTest {

    private TimescaleDb<SourceRecord> transformation;

    @Before
    public void initTransformation() {
        transformation = new TimescaleDb<>();
        transformation.setMetadata(new TestMetadata(Configuration.from(Collections.emptyMap())));
        transformation.configure(Collections.emptyMap());
    }

    @Test
    public void shouldProcessHypertable() {
        final var record = createSourceRecord("_timescaledb_internal", "_hyper_1_1_chunk", 1);
        final var transformed = transformation.apply(record);
        final var source = ((Struct) transformed.value()).getStruct("source");

        assertThat(source.getString("schema")).isEqualTo("public");
        assertThat(source.getString("table")).isEqualTo("conditions");
        assertThat(transformed.topic()).isEqualTo("timescaledb.public.conditions");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_chunk_schema").value()).isEqualTo("_timescaledb_internal");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_chunk_table").value()).isEqualTo("_hyper_1_1_chunk");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_hypertable_schema")).isNull();
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_hypertable_table")).isNull();
    }

    @Test
    public void shouldRouteToDifferentTopic() {
        final var config = Map.of("target.topic.prefix", "myprefix");
        transformation = new TimescaleDb<>();
        transformation.setMetadata(new TestMetadata(Configuration.from(config)));
        transformation.configure(config);

        final var record = createSourceRecord("_timescaledb_internal", "_hyper_1_1_chunk", 1);
        final var transformed = transformation.apply(record);
        final var source = ((Struct) transformed.value()).getStruct("source");

        assertThat(source.getString("schema")).isEqualTo("public");
        assertThat(source.getString("table")).isEqualTo("conditions");
        assertThat(transformed.topic()).isEqualTo("myprefix.public.conditions");
    }

    @Test
    public void shouldProcessAggregate() {
        final var record = createSourceRecord("_timescaledb_internal", "_hyper_2_2_chunk", 1);
        final var transformed = transformation.apply(record);
        final var source = ((Struct) transformed.value()).getStruct("source");

        assertThat(source.getString("schema")).isEqualTo("public");
        assertThat(source.getString("table")).isEqualTo("conditions_summary");
        assertThat(transformed.topic()).isEqualTo("timescaledb.public.conditions_summary");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_chunk_schema").value()).isEqualTo("_timescaledb_internal");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_chunk_table").value()).isEqualTo("_hyper_2_2_chunk");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_hypertable_schema").value()).isEqualTo("_timescaledb_internal");
        assertThat(transformed.headers().lastWithName("__debezium_timescaledb_hypertable_table").value()).isEqualTo("_materialized_hypertable_2");
    }

    @Test
    public void shouldIgnoreMessagesWithoutRequiredSource() {
        final var record1 = createSourceRecord(null, null, 1);
        final var transformed1 = transformation.apply(record1);
        assertThat(transformed1).isSameAs(record1);

        final var record2 = createSourceRecord("_timescaledb_internal", null, 2);
        final var transformed2 = transformation.apply(record2);
        assertThat(transformed2).isSameAs(record2);

        final var record3 = createSourceRecord(null, "_hyper_1_1_chunk", 3);
        final var transformed3 = transformation.apply(record3);
        assertThat(transformed3).isSameAs(record3);
    }

    @Test
    public void shouldIgnoreMessagesForNonTimescaleSchema() {
        final var record = createSourceRecord("different", "table", 1);
        final var transformed = transformation.apply(record);
        assertThat(transformed).isSameAs(record);
    }

    @Test
    public void shouldWarnOnOrphanChunk() {
        final var logInterceptor = new LogInterceptor(TimescaleDb.class);
        final var record = createSourceRecord("_timescaledb_internal", "_hyper_10_1_chunk", 1);
        final var transformed = transformation.apply(record);
        assertThat(transformed).isSameAs(record);
        logInterceptor.containsWarnMessage("Unable to find hypertable for chunk '_timescaledb_internal._hyper_10_1_chunk'");
    }

    private SourceRecord createSourceRecord(String schema, String table, long id) {

        final var valueSchema = SchemaBuilder.struct()
                .name("server1.timescaledb.Value")
                .field("id", Schema.INT64_SCHEMA)
                .build();

        final var sourceSchema = SchemaBuilder.struct()
                .name("source")
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final var envelope = Envelope.defineSchema()
                .withName("server1.timescaledb.Envelope")
                .withRecord(valueSchema)
                .withSource(sourceSchema)
                .build();

        final var source = new Struct(sourceSchema);
        if (schema != null) {
            source.put("schema", schema);
        }
        if (table != null) {
            source.put("table", table);
        }

        final var value = new Struct(valueSchema);
        value.put("id", id);

        Struct payload = envelope.create(value, ((schema == null) && (table == null)) ? null : source, Instant.now());

        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "server1.timescaledb",
                envelope.schema(), payload);
    }
}
