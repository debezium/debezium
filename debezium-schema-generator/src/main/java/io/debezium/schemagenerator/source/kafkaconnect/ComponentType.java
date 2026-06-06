/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.source.kafkaconnect;

/**
 * Enumeration of Kafka Connect component types that can be discovered.
 *
 * <p>Each type corresponds to a specific Kafka Connect interface:
 * <ul>
 *   <li>{@link #TRANSFORMATION} - org.apache.kafka.connect.transforms.Transformation</li>
 *   <li>{@link #CONVERTER} - org.apache.kafka.connect.storage.Converter</li>
 *   <li>{@link #HEADER_CONVERTER} - org.apache.kafka.connect.storage.HeaderConverter</li>
 *   <li>{@link #PREDICATE} - org.apache.kafka.connect.transforms.predicates.Predicate</li>
 * </ul>
 */
public enum ComponentType {

    /**
     * Single Message Transformations (SMTs) that modify records in a Kafka Connect pipeline.
     */
    TRANSFORMATION("transformation", "Transformation"),

    /**
     * Converters that serialize/deserialize record keys and values.
     */
    CONVERTER("converter", "Converter"),

    /**
     * Converters that serialize/deserialize record headers.
     */
    HEADER_CONVERTER("header-converter", "HeaderConverter"),

    /**
     * Predicates used for conditional transformations.
     */
    PREDICATE("predicate", "Predicate");

    private final String id;
    private final String displayName;

    ComponentType(String id, String displayName) {
        this.id = id;
        this.displayName = displayName;
    }

    /**
     * Returns the component type identifier (lowercase, hyphenated).
     *
     * @return type ID, e.g. "transformation", "converter"
     */
    public String getId() {
        return id;
    }

    /**
     * Returns a human-readable display name for logging and error messages.
     *
     * @return display name, e.g. "Transformation", "Converter"
     */
    public String getDisplayName() {
        return displayName;
    }
}
