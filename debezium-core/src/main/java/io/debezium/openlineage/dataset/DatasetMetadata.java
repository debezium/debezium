/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.List;

public record DatasetMetadata(String name, DatasetType type, List<FieldDefinition> fields) {

    public enum DatasetType {
        INPUT,
        OUTPUT
    }

    public record FieldDefinition(String name, String typeName, String description, List<FieldDefinition> fields) {

        public FieldDefinition(String name, String typeName, String description) {
            this(name, typeName, description, List.of());
        }
    }
}
