/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.fest.assertions.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.data.Envelope;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.Signal.Payload;
import io.debezium.relational.TableId;

/**
 * @author Jiri Pechanec
 *
 */
public class SignalTest {

    @Test
    public void shouldDetectSignal() {
        final Signal signal = new Signal(config());
        assertThat(signal.isSignal(new TableId("dbo", null, "mytable"))).isFalse();
        assertThat(signal.isSignal(new TableId("debezium", null, "signal"))).isTrue();
    }

    @Test
    public void shouldExecuteLog() throws Exception {
        final Signal signal = new Signal(config());
        final LogInterceptor log = new LogInterceptor(io.debezium.pipeline.signal.Log.class);
        assertThat(signal.process("log1", "log", "{\"message\": \"signallog {}\"}")).isTrue();
        assertThat(log.containsMessage("signallog <none>")).isTrue();
    }

    @Test
    public void shouldIgnoreInvalidSignalType() throws Exception {
        final Signal signal = new Signal(config());
        assertThat(signal.process("log1", "log1", "{\"message\": \"signallog\"}")).isFalse();
    }

    @Test
    public void shouldIgnoreUnparseableData() throws Exception {
        final Signal signal = new Signal(config());
        assertThat(signal.process("log1", "log", "{\"message: \"signallog\"}")).isFalse();
    }

    @Test
    public void shouldRegisterAdditionalAction() throws Exception {
        final Signal signal = new Signal(config());

        final AtomicInteger called = new AtomicInteger();
        final Signal.Action testAction = new Signal.Action() {

            @Override
            public boolean arrived(Payload signalPayload) {
                called.set(signalPayload.data.getInteger("v"));
                return true;
            }
        };
        signal.registerSignalAction("custom", testAction);
        assertThat(signal.process("log1", "custom", "{\"v\": 5}")).isTrue();
        assertThat(called.intValue()).isEqualTo(5);
    }

    @Test
    public void shouldExecuteFromEnvelope() throws Exception {
        final Signal signal = new Signal(config());
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
        final AtomicInteger called = new AtomicInteger();
        final Signal.Action testAction = new Signal.Action() {

            @Override
            public boolean arrived(Payload signalPayload) {
                called.set(signalPayload.data.getInteger("v"));
                return true;
            }
        };
        signal.registerSignalAction("custom", testAction);
        assertThat(signal.process(env.create(record, null, null), null)).isTrue();
        assertThat(called.intValue()).isEqualTo(5);
    }

    @Test
    public void shouldIgnoreInvalidEnvelope() throws Exception {
        final Signal signal = new Signal(config());
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
        final AtomicInteger called = new AtomicInteger();
        final Signal.Action testAction = new Signal.Action() {

            @Override
            public boolean arrived(Payload signalPayload) {
                called.set(signalPayload.data.getInteger("v"));
                return true;
            }
        };
        signal.registerSignalAction("custom", testAction);

        assertThat(signal.process(env.create(record, null, null), null)).isFalse();
        assertThat(called.intValue()).isEqualTo(0);

        assertThat(signal.process(record, null)).isFalse();
        assertThat(called.intValue()).isEqualTo(0);
    }

    protected CommonConnectorConfig config() {
        return new CommonConnectorConfig(Configuration.create().with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal").build(), "core", 0) {
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
        };
    }
}
