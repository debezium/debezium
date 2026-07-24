/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dlq;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * Factory methods for {@link ErrorReporter} instances backed by the Kafka Connect
 * errant record reporter (KIP-610).
 */
public final class ErrorReporters {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorReporters.class);

    private static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    private static final String ERRORS_DLQ_TOPIC_NAME_CONFIG = "errors.deadletterqueue.topic.name";
    private static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";

    private static final ErrorReporter NOP = (record, e) -> {
    };

    private ErrorReporters() {
    }

    /**
     * Creates an {@link ErrorReporter} backed by the errant record reporter of the supplied sink task
     * context. Falls back to a no-op reporter when the runtime does not provide one, either because
     * error reporting is not configured for the connector or because the Connect runtime predates
     * Kafka 2.6.
     *
     * @param context the sink task context; may be null
     * @return the error reporter; never null
     */
    public static ErrorReporter fromContext(SinkTaskContext context) {
        ErrorReporter result = nop();
        if (context != null) {
            try {
                ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
                if (errantRecordReporter != null) {
                    result = (DebeziumSinkRecord record, Exception e) -> {
                        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
                            errantRecordReporter.report(kafkaRecord.getOriginalKafkaRecord(), e);
                        }
                    };
                }
                else {
                    LOGGER.info("Errant record reporter not configured.");
                }
            }
            catch (NoClassDefFoundError | NoSuchMethodError e) {
                // Will occur in Connect runtimes earlier than 2.6
                LOGGER.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        }
        return result;
    }

    /**
     * Returns the no-op reporter that silently discards all reported records.
     */
    public static ErrorReporter nop() {
        return NOP;
    }

    /**
     * Returns whether the supplied reporter routes records anywhere, i.e. is not the no-op reporter.
     */
    public static boolean isEnabled(ErrorReporter reporter) {
        return reporter != null && reporter != NOP;
    }

    /**
     * Validates the standard Kafka Connect error handling options of the connector configuration.
     * <p>
     * A dead letter queue topic combined with {@code errors.tolerance=none} is rejected, because
     * records can never be routed to the dead letter queue with that combination. A tolerance of
     * {@code all} without any configured error reporter is only logged as a warning, because it is
     * a legitimate way to skip records that fail in the converter or transformation stages.
     *
     * @param reporter the reporter obtained from {@link #fromContext(SinkTaskContext)}
     * @param connectorProps the connector configuration properties
     * @throws ConnectException if the configuration is contradictory
     */
    public static void validateConfiguration(ErrorReporter reporter, Map<String, String> connectorProps) {
        final String tolerance = connectorProps.getOrDefault(ERRORS_TOLERANCE_CONFIG, "none").trim();
        final String dlqTopic = connectorProps.getOrDefault(ERRORS_DLQ_TOPIC_NAME_CONFIG, "").trim();
        final boolean toleranceAll = "all".equalsIgnoreCase(tolerance);

        if (!dlqTopic.isEmpty() && !toleranceAll) {
            throw new ConnectException(String.format(
                    "'%s' is configured but '%s' is '%s'; records can never be routed to the dead letter queue. "
                            + "Set '%s' to 'all' or remove the dead letter queue configuration.",
                    ERRORS_DLQ_TOPIC_NAME_CONFIG, ERRORS_TOLERANCE_CONFIG, tolerance, ERRORS_TOLERANCE_CONFIG));
        }
        if (toleranceAll && !isEnabled(reporter)) {
            LOGGER.warn("'{}' is 'all' but neither '{}' nor '{}' is configured; records that fail to be written will still stop the task.",
                    ERRORS_TOLERANCE_CONFIG, ERRORS_DLQ_TOPIC_NAME_CONFIG, ERRORS_LOG_ENABLE_CONFIG);
        }
    }
}
