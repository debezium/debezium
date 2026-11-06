/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.HasHeaderKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

public class TransformationsTracingTest {

    private static final String SAMPLED_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SAMPLED_SPAN_ID = "00f067aa0ba902b7";
    private static final String OTHER_TRACE_ID = "5bf92f3577b34da6a3ce929d0e0e4737";
    private static final String OTHER_SPAN_ID = "10f067aa0ba902b8";
    private static final String UNSAMPLED_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String UNSAMPLED_SPAN_ID = "b7ad6b7169203331";
    private static final long SLOW_MILLIS = 100;

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    @BeforeEach
    void installSdk() {
        GlobalOpenTelemetry.resetForTest();
        OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .setSampler(Sampler.parentBased(Sampler.alwaysOn()))
                        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                        .build())
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();
        CapturingTransformation.CAPTURED.clear();
    }

    @AfterEach
    void resetSdk() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void shouldTraceEachTransformationWithinTheRecordContext() throws Exception {
        try (Transformations transformations = new Transformations(config("a,b", true, null))) {
            transformations.transform(record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"));
            transformations.transform(record(OTHER_TRACE_ID, OTHER_SPAN_ID, "01"));
            transformations.transform(record(UNSAMPLED_TRACE_ID, UNSAMPLED_SPAN_ID, "00"));
        }

        final List<SpanData> spans = exporter.getFinishedSpanItems();
        assertThat(spans).extracting(SpanData::getName)
                .containsExactly("debezium-transform a", "debezium-transform b", "debezium-transform a", "debezium-transform b");
        assertThat(spans.subList(0, 2)).allSatisfy(span -> assertChildOf(span, SAMPLED_TRACE_ID, SAMPLED_SPAN_ID));
        assertThat(spans.subList(2, 4)).allSatisfy(span -> assertChildOf(span, OTHER_TRACE_ID, OTHER_SPAN_ID));
        assertThat(spans).allSatisfy(span -> {
            assertThat(span.getAttributes().get(TracingTransformation.TYPE)).isEqualTo(CapturingTransformation.class.getName());
            assertThat(span.getAttributes().get(TracingTransformation.OUTCOME)).isEqualTo(TracingTransformation.OUTCOME_PASSED);
        });
        assertThat(spans.get(0).getAttributes().get(TracingTransformation.ALIAS)).isEqualTo("a");

        final List<SpanContext> captured = CapturingTransformation.CAPTURED;
        assertThat(captured).hasSize(6);
        assertThat(captured.get(0).getSpanId()).isEqualTo(spans.get(0).getSpanId());
        assertThat(captured.get(4).getTraceId()).isEqualTo(UNSAMPLED_TRACE_ID);
        assertThat(captured.get(4).isSampled()).isFalse();
        assertThat(Span.current().getSpanContext().isValid()).isFalse();
    }

    @Test
    void shouldMeasureOnlyTheTransformationExecution() throws Exception {
        final SourceRecord record = record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01");
        TimeUnit.MILLISECONDS.sleep(SLOW_MILLIS);
        final long beforeInvocation = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());

        try (Transformations transformations = new Transformations(config("fast,slow", true, null))) {
            transformations.transform(record);
        }

        final SpanData fast = span("debezium-transform fast");
        final SpanData slow = span("debezium-transform slow");
        assertThat(fast.getStartEpochNanos()).isGreaterThanOrEqualTo(beforeInvocation);
        assertThat(slow.getEndEpochNanos() - slow.getStartEpochNanos()).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(SLOW_MILLIS));
        assertThat(fast.getEndEpochNanos() - fast.getStartEpochNanos()).isLessThan(TimeUnit.MILLISECONDS.toNanos(SLOW_MILLIS));
    }

    @Test
    void shouldTraceOnlyIncludedAliases() throws Exception {
        try (Transformations transformations = new Transformations(config("a,b", true, "b"))) {
            transformations.transform(record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"));
        }

        assertThat(exporter.getFinishedSpanItems()).extracting(SpanData::getName).containsExactly("debezium-transform b");
    }

    @Test
    void shouldNotTraceWhenDisabled() throws Exception {
        try (Transformations transformations = new Transformations(config("a,b", false, "a,b"))) {
            final SourceRecord result = transformations.transform(record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"));
            assertThat(result).isNotNull();
        }

        assertThat(exporter.getFinishedSpanItems()).isEmpty();
        assertThat(CapturingTransformation.CAPTURED).hasSize(2).noneMatch(SpanContext::isValid);
    }

    @Test
    void shouldNotStartRootSpansForRecordsWithoutTraceContext() throws Exception {
        try (Transformations transformations = new Transformations(config("a", true, null))) {
            assertThat(transformations.transform(recordWithHeaders(new ConnectHeaders()))).isNotNull();
        }

        assertThat(exporter.getFinishedSpanItems()).isEmpty();
        assertThat(CapturingTransformation.CAPTURED).hasSize(1);
    }

    @Test
    void shouldRecordFilteredRecordsAndStopTheChain() throws Exception {
        try (Transformations transformations = new Transformations(config("drop,a", true, null))) {
            assertThat(transformations.transform(record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"))).isNull();
        }

        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
            assertThat(span.getName()).isEqualTo("debezium-transform drop");
            assertThat(span.getAttributes().get(TracingTransformation.OUTCOME)).isEqualTo(TracingTransformation.OUTCOME_FILTERED);
        });
        assertThat(CapturingTransformation.CAPTURED).isEmpty();
    }

    @Test
    void shouldEndSpanAndRestoreContextWhenTransformationFails() throws Exception {
        try (Transformations transformations = new Transformations(config("fail,a", true, null))) {
            assertThatThrownBy(() -> transformations.transform(record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("boom");
        }

        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
            assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(span.getAttributes().get(TracingTransformation.OUTCOME)).isEqualTo(TracingTransformation.OUTCOME_FAILED);
            assertThat(span.getAttributes().get(TracingTransformation.ERROR_TYPE)).isEqualTo(IllegalStateException.class.getName());
            assertThat(span.getEvents()).isEmpty();
        });
        assertThat(Span.current().getSpanContext().isValid()).isFalse();
    }

    @Test
    void shouldNotTraceTransformationsSkippedByTheirPredicate() throws Exception {
        final Properties properties = properties("a", true, null);
        properties.setProperty("predicates", "marked");
        properties.setProperty("predicates.marked.type", HasHeaderKey.class.getName());
        properties.setProperty("predicates.marked.name", "marker");
        properties.setProperty("transforms.a.predicate", "marked");

        try (Transformations transformations = new Transformations(Configuration.from(properties))) {
            transformations.transform(record(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"));
        }

        assertThat(exporter.getFinishedSpanItems()).isEmpty();
        assertThat(CapturingTransformation.CAPTURED).isEmpty();
    }

    private static void assertChildOf(SpanData span, String traceId, String parentSpanId) {
        assertThat(span.getTraceId()).isEqualTo(traceId);
        assertThat(span.getParentSpanId()).isEqualTo(parentSpanId);
    }

    private SpanData span(String name) {
        return exporter.getFinishedSpanItems().stream().filter(span -> span.getName().equals(name)).findFirst().orElseThrow();
    }

    private static Configuration config(String transforms, boolean enabled, String include) {
        return Configuration.from(properties(transforms, enabled, include));
    }

    private static Properties properties(String transforms, boolean enabled, String include) {
        final Properties properties = new Properties();
        properties.setProperty("transforms", transforms);
        properties.setProperty("transforms.a.type", CapturingTransformation.class.getName());
        properties.setProperty("transforms.b.type", CapturingTransformation.class.getName());
        properties.setProperty("transforms.fast.type", CapturingTransformation.class.getName());
        properties.setProperty("transforms.slow.type", SlowTransformation.class.getName());
        properties.setProperty("transforms.drop.type", DroppingTransformation.class.getName());
        properties.setProperty("transforms.fail.type", FailingTransformation.class.getName());
        properties.setProperty(EmbeddedEngineConfig.TRANSFORMS_TRACING_ENABLED.name(), String.valueOf(enabled));
        if (include != null) {
            properties.setProperty(EmbeddedEngineConfig.TRANSFORMS_TRACING_INCLUDE.name(), include);
        }
        return properties;
    }

    private static SourceRecord record(String traceId, String spanId, String flags) {
        return recordWithHeaders(new ConnectHeaders().addString("traceparent", "00-" + traceId + "-" + spanId + "-" + flags));
    }

    private static SourceRecord recordWithHeaders(Headers headers) {
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "topic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "value",
                System.currentTimeMillis(), headers);
    }

    public abstract static class TestTransformation implements Transformation<SourceRecord> {
        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public void close() {
        }
    }

    public static class CapturingTransformation extends TestTransformation {
        static final List<SpanContext> CAPTURED = Collections.synchronizedList(new ArrayList<>());

        @Override
        public SourceRecord apply(SourceRecord record) {
            CAPTURED.add(Span.current().getSpanContext());
            return record;
        }
    }

    public static class SlowTransformation extends TestTransformation {
        @Override
        public SourceRecord apply(SourceRecord record) {
            try {
                TimeUnit.MILLISECONDS.sleep(SLOW_MILLIS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return record;
        }
    }

    public static class DroppingTransformation extends TestTransformation {
        @Override
        public SourceRecord apply(SourceRecord record) {
            return null;
        }
    }

    public static class FailingTransformation extends TestTransformation {
        @Override
        public SourceRecord apply(SourceRecord record) {
            throw new IllegalStateException("boom");
        }
    }
}
