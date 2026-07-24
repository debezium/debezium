/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.doc.FixFor;

class ErrorReportersTest {

    @FixFor("debezium/dbz#984")
    @Test
    void nopReporterShouldBeDisabled() {
        assertThat(ErrorReporters.isEnabled(ErrorReporters.nop())).isFalse();
        assertThat(ErrorReporters.isEnabled(null)).isFalse();
    }

    @FixFor("debezium/dbz#984")
    @Test
    void fromNullContextShouldReturnNop() {
        assertThat(ErrorReporters.isEnabled(ErrorReporters.fromContext(null))).isFalse();
    }

    @FixFor("debezium/dbz#984")
    @Test
    void fromContextWithoutReporterShouldReturnNop() {
        ErrorReporter reporter = ErrorReporters.fromContext(new TestSinkTaskContext(null));

        assertThat(ErrorReporters.isEnabled(reporter)).isFalse();
    }

    @FixFor("debezium/dbz#984")
    @Test
    void fromContextWithReporterShouldRouteOriginalKafkaRecord() {
        List<SinkRecord> reported = new ArrayList<>();
        ErrantRecordReporter errantRecordReporter = (record, error) -> {
            reported.add(record);
            return CompletableFuture.completedFuture(null);
        };
        ErrorReporter reporter = ErrorReporters.fromContext(new TestSinkTaskContext(errantRecordReporter));

        assertThat(ErrorReporters.isEnabled(reporter)).isTrue();

        Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("id", (byte) 1);
        SinkRecord kafkaRecord = new SinkRecord("topic", 0, null, null, valueSchema, value, 42);
        reporter.report(new KafkaDebeziumSinkRecord(kafkaRecord, null), new RuntimeException("boom"));

        assertThat(reported).hasSize(1);
        assertThat(reported.get(0).kafkaOffset()).isEqualTo(42);
    }

    @FixFor("debezium/dbz#984")
    @Test
    void validateShouldRejectDlqTopicWithoutToleranceAll() {
        Map<String, String> props = Map.of(
                "errors.deadletterqueue.topic.name", "dlq-topic");

        assertThatThrownBy(() -> ErrorReporters.validateConfiguration(ErrorReporters.nop(), props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("errors.deadletterqueue.topic.name")
                .hasMessageContaining("Set 'errors.tolerance' to 'all'");

        Map<String, String> explicitNone = Map.of(
                "errors.tolerance", "none",
                "errors.deadletterqueue.topic.name", "dlq-topic");

        assertThatThrownBy(() -> ErrorReporters.validateConfiguration(ErrorReporters.nop(), explicitNone))
                .isInstanceOf(ConnectException.class);
    }

    @FixFor("debezium/dbz#984")
    @Test
    void validateShouldAcceptDlqTopicWithToleranceAll() {
        Map<String, String> props = Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", "dlq-topic");

        ErrorReporters.validateConfiguration((record, e) -> {
        }, props);

        Map<String, String> upperCase = Map.of(
                "errors.tolerance", "ALL",
                "errors.deadletterqueue.topic.name", "dlq-topic");

        ErrorReporters.validateConfiguration((record, e) -> {
        }, upperCase);
    }

    @FixFor("debezium/dbz#984")
    @Test
    void validateShouldOnlyWarnWhenToleranceAllHasNoReporter() {
        // errors.tolerance=all without a DLQ topic or error log is a legitimate way to skip
        // converter/transformation stage failures, so it must not fail the startup.
        ErrorReporters.validateConfiguration(ErrorReporters.nop(), Map.of("errors.tolerance", "all"));
    }

    @FixFor("debezium/dbz#984")
    @Test
    void validateShouldAcceptDefaultErrorHandlingConfiguration() {
        ErrorReporters.validateConfiguration(ErrorReporters.nop(), Map.of());
    }

    private static class TestSinkTaskContext implements SinkTaskContext {

        private final ErrantRecordReporter reporter;

        TestSinkTaskContext(ErrantRecordReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public Map<String, String> configs() {
            return Map.of();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
        }

        @Override
        public void timeout(long timeoutMs) {
        }

        @Override
        public Set<TopicPartition> assignment() {
            return Set.of();
        }

        @Override
        public void pause(TopicPartition... partitions) {
        }

        @Override
        public void resume(TopicPartition... partitions) {
        }

        @Override
        public void requestCommit() {
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            return reporter;
        }

        @Override
        public PluginMetrics pluginMetrics() {
            return null;
        }
    }

}
