/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.List;

import io.debezium.spi.schema.DataCollectionId;

public record DataCollectionMetadata(DataCollectionId id, List<FieldDefinition> fields) {

    public record FieldDefinition(String name, String typeName, String description) {
    }
}
