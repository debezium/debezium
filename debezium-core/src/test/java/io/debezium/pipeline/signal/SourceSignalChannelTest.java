/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.data.Envelope;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;

/**
 * @author Jiri Pechanec
 *
 */
public class SourceSignalChannelTest {

    @Test
    public void shouldExecuteFromEnvelope() throws Exception {

        final SourceSignalChannel sourceSignalChannel = new SourceSignalChannel();
        sourceSignalChannel.init(config());
        final Schema afterSchema = SchemaBuilder.struct().name("signal")
                .field("col1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("col2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("col3", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Envelope env = Envelope.defineSchema()
                .withName("someName")
                .withRecord(afterSchema)
                .withSource(SchemaBuilder.struct().name("source").build())
                .build();
        final Struct record = new Struct(afterSchema);
        record.put("col1", "log1");
        record.put("col2", "custom");
        record.put("col3", "{\"v\": 5}");

        sourceSignalChannel.process(env.create(record, null, null));
        List<SignalRecord> signalRecords = sourceSignalChannel.read();
        assertThat(signalRecords).hasSize(1);
        assertThat(signalRecords.get(0).getData()).isEqualTo("{\"v\": 5}");
    }

    @Test
    public void shouldIgnoreInvalidEnvelope() throws Exception {

        final SourceSignalChannel sourceSignalChannel = new SourceSignalChannel();
        sourceSignalChannel.init(config());
        final Schema afterSchema = SchemaBuilder.struct().name("signal")
                .field("col1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("col2", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Envelope env = Envelope.defineSchema()
                .withName("someName")
                .withRecord(afterSchema)
                .withSource(SchemaBuilder.struct().name("source").build())
                .build();
        final Struct record = new Struct(afterSchema);
        record.put("col1", "log1");
        record.put("col2", "custom");

        sourceSignalChannel.process(env.create(record, null, null));
        assertThat(sourceSignalChannel.read()).hasSize(0);

        sourceSignalChannel.process(record);
        assertThat(sourceSignalChannel.read()).hasSize(0);
    }

    protected CommonConnectorConfig config() {
        return new CommonConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "core")
                .build(), 0) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }

}
