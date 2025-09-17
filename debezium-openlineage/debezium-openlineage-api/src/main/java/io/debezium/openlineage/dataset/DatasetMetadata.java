/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.List;

/**
 * Represents metadata for a dataset, including its name, kind, type, and field definitions.
 * This record provides a complete description of a dataset's structure and characteristics.
 *
 * @param name   the name of the dataset
 * @param kind   the kind of the dataset (INPUT or OUTPUT)
 * @param type   the type of the dataset (e.g. TABLE, VIEW, TOPIC, MODEL). Maps to <a href="https://openlineage.io/docs/spec/facets/dataset-facets/type">OpenLineage dataset type</a>
 * @param fields the list of field definitions that make up the dataset structure
 */
public record DatasetMetadata(String name, DatasetKind kind, String type, DataStore store, List<FieldDefinition> fields) {

    public static final String TABLE_DATASET_TYPE = "TABLE";
    public static final String STREAM_DATASET_TYPE = "STREAM";

    /**
     * Enumeration representing the kind of dataset.
     * Datasets can be classified as either input sources or output destinations.
     */
    public enum DatasetKind {
        /** Represents a dataset that serves as an input source */
        INPUT,
        /** Represents a dataset that serves as an output destination */
        OUTPUT
    }

    /**
     * Enumeration representing the final data store related to the dataset.
     * Relates to <a href="https://openlineage.io/docs/spec/naming">OpenLineage DataSet naming</a>
     */
    public enum DataStore {
        KAFKA,
        DATABASE
    }

    /**
     * Represents the definition of a field within a dataset.
     * A field can be simple (primitive kind) or complex (containing nested fields).
     *
     * @param name        the name of the field
     * @param typeName    the kind name of the field
     * @param description a human-readable description of the field's purpose or content
     * @param fields      nested field definitions for complex types (empty for primitive types)
     */
    public record FieldDefinition(String name, String typeName, String description, List<FieldDefinition> fields) {

        /**
         * Convenience constructor for creating a simple field definition without nested fields.
         * This is typically used for primitive types that don't contain sub-fields.
         *
         * @param name        the name of the field
         * @param typeName    the kind name of the field
         * @param description a human-readable description of the field
         */
        public FieldDefinition(String name, String typeName, String description) {
            this(name, typeName, description, List.of());
        }
    }
}
