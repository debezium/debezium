/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import java.util.List;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class DatasetDataExtractor {

    public <R extends ConnectRecord<R>> List<DatasetMetadata.FieldDefinition> extract(ConnectRecord<R> record) {

        if (record.valueSchema() == null || record.valueSchema().type() != Schema.Type.STRUCT) {
            return List.of();
        }

        return record.valueSchema().fields().stream()
                .map(this::buildFieldDefinition)
                .toList();
    }

    private DatasetMetadata.FieldDefinition buildFieldDefinition(Field field) {

        Schema schema = field.schema();
        String name = field.name();
        String typeName = schema.type().name();
        String description = schema.doc();

        if (schema.type() == Schema.Type.STRUCT && schema.fields() != null && !schema.fields().isEmpty()) {

            List<DatasetMetadata.FieldDefinition> nestedFields = schema.fields().stream()
                    .map(this::buildFieldDefinition)
                    .toList();

            return new DatasetMetadata.FieldDefinition(name, typeName, description, nestedFields);
        }

        return new DatasetMetadata.FieldDefinition(name, typeName, description);
    }
}
