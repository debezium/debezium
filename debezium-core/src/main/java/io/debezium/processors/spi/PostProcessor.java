/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.spi;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.common.annotation.Incubating;

/**
 * A contract that defines a post-processing step that can be applied to any outgoing event
 * before it is added to the change event queue.
 *
 * While this may seem similar to a Kafka Transformation, the difference is that a post processor
 * operates on the raw {@link Struct} objects which are still mutable, allowing for a variety of
 * use cases that wish to have the full data set of a row for the post-processing step.
 *
 * In additoin, there are several injection-aware contracts that can be used in conjunction with
 * the post processor to automatically have specific Debezium internal objects injected into the
 * post processor automatically at connector start-up.
 *
 * @author Chris Cranford
 *
 * @see ConnectorConfigurationAware
 * @see JdbcConnectionAware
 * @see RelationalDatabaseSchemaAware
 * @see ValueConverterAware
 */
@Incubating
public interface PostProcessor extends AutoCloseable {
    /**
     * Configure the post processor.
     *
     * @param properties map of configurable properties
     */
    void configure(Map<String, ?> properties);

    /**
     * Apply the post processor to the supplied key and value.
     *
     * @param key the event's key, may be {@code null}
     * @param value the event's value, may be {@code null}
     */
    void apply(Object key, Struct value);

    /**
     * Close any resources
     */
    @Override
    void close();
}
