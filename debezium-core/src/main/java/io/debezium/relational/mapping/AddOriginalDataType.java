/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import org.apache.kafka.connect.data.SchemaBuilder;

public class AddOriginalDataType implements ColumnMapper {
    @Override
    public ValueConverter create(Column column) {
       return null;
    }

    @Override
    public void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
       schemaBuilder.parameter("originalType", column.typeName().toUpperCase());
    }
}
