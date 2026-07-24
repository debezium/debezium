/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dlq;

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
}
