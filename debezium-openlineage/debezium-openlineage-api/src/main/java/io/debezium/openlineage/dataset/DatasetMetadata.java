/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.List;

/**
 * Represents metadata for a dataset, including its name, type, and field definitions.
 * This record provides a complete description of a dataset's structure and characteristics.
 *
 * @param name   the name of the dataset
 * @param type   the type of the dataset (INPUT or OUTPUT)
 * @param fields the list of field definitions that make up the dataset structure
 */
public record DatasetMetadata(String name, DatasetType type, List<FieldDefinition> fields) {

    /**
     * Enumeration representing the type of dataset.
     * Datasets can be classified as either input sources or output destinations.
     */
    public enum DatasetType {
        /** Represents a dataset that serves as an input source */
        INPUT,
        /** Represents a dataset that serves as an output destination */
        OUTPUT
    }

    /**
     * Represents the definition of a field within a dataset.
     * A field can be simple (primitive type) or complex (containing nested fields).
     *
     * @param name        the name of the field
     * @param typeName    the type name of the field
     * @param description a human-readable description of the field's purpose or content
     * @param fields      nested field definitions for complex types (empty for primitive types)
     */
    public record FieldDefinition(String name, String typeName, String description, List<FieldDefinition> fields) {

        /**
         * Convenience constructor for creating a simple field definition without nested fields.
         * This is typically used for primitive types that don't contain sub-fields.
         *
         * @param name        the name of the field
         * @param typeName    the type name of the field
         * @param description a human-readable description of the field
         */
        public FieldDefinition(String name, String typeName, String description) {
            this(name, typeName, description, List.of());
        }
    }
}
