/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceTask;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.DebeziumEngine;

/**
 * Extension to {@link DebeziumEngine}, which provide convenient methods used in testsuite which we want to keep either for backward compatibility or because
 * they are just useful, but we don't want to expose them via {@link DebeziumEngine} API.
 * Amount of these method should be kept as minimal as possible.
 */
@Incubating
public interface TestingDebeziumEngine<T> extends DebeziumEngine<T> {

    /**
     * Run consumer function with engine task, e.g. in case of Kafka with {@link SourceTask}.
     * Effectively expose engine internal task for testing.
     */
    void runWithTask(Consumer<SourceTask> consumer);
}
