/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

import io.debezium.config.Field;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;

/**
 * ComponentMetadata implementation for Kafka Connect components.
 *
 * <p>This implementation wraps a Kafka Connect component's configuration
 * as a Debezium ComponentMetadata, allowing KC components to be processed
 * using the same schema generation pipeline as Debezium components.
 *
 * <p>Since KC components don't have a built-in version mechanism, this
 * implementation uses "1.0.0" as a default version for all components.
 */
public class KafkaConnectComponentMetadata implements ComponentMetadata {

    private static final String DEFAULT_VERSION = "1.0.0";

    private final ComponentDescriptor componentDescriptor;
    private final Field.Set componentFields;

    /**
     * Creates metadata for a Kafka Connect component.
     *
     * @param componentClass the KC component class
     * @param fields the component's configuration fields
     */
    public KafkaConnectComponentMetadata(Class<?> componentClass, Field.Set fields) {
        this.componentDescriptor = new ComponentDescriptor(
                componentClass.getName(),
                DEFAULT_VERSION);
        this.componentFields = fields;
    }

    @Override
    public ComponentDescriptor getComponentDescriptor() {
        return componentDescriptor;
    }

    @Override
    public Field.Set getComponentFields() {
        return componentFields;
    }
}
