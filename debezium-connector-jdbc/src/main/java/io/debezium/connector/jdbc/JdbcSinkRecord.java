/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.SchemaUtils;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * @author rk3rn3r
 */
@Immutable
public interface JdbcSinkRecord extends DebeziumSinkRecord {

    List<String> getNonKeyFieldNames();

    Map<String, FieldDescriptor> allFields();

    /**
     * An immutable representation of a {@link Field} in a {@link SinkRecord}.
     *
     * @author Chris Cranford
     */
    @Immutable
    class FieldDescriptor {

        private static final Logger LOGGER = LoggerFactory.getLogger(FieldDescriptor.class);

        private final Schema schema;
        private final String name;
        private final String columnName;
        private final boolean isKey;
        private final Type type;
        private final String typeName;

        // Lazily prepared
        private String queryBinding;

        protected FieldDescriptor(Schema schema, String name, boolean isKey, DatabaseDialect dialect) {
            this.schema = schema;
            this.isKey = isKey;

            // These are cached here allowing them to be resolved once per record
            this.type = dialect.getSchemaType(schema);
            this.typeName = type.getTypeName(dialect, schema, isKey);

            this.name = name;
            this.columnName = SchemaUtils.getSourceColumnName(schema).orElse(name);

            LOGGER.trace("Field [{}] with schema [{}]", this.name, schema.type());
            LOGGER.trace("    Type      : {}", type.getClass().getName());
            LOGGER.trace("    Type Name : {}", typeName);
            LOGGER.trace("    Optional  : {}", schema.isOptional());

            if (schema.parameters() != null && !schema.parameters().isEmpty()) {
                LOGGER.trace("    Parameters: {}", schema.parameters());
            }

            if (schema.defaultValue() != null) {
                LOGGER.trace("    Def. Value: {}", schema.defaultValue());
            }
        }

        public Schema getSchema() {
            return schema;
        }

        public String getName() {
            return name;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isKey() {
            return isKey;
        }

        public Type getType() {
            return type;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getQueryBinding(ColumnDescriptor column, Object value) {
            if (queryBinding == null) {
                queryBinding = type.getQueryBinding(column, schema, value);
            }
            return queryBinding;
        }

        public List<ValueBindDescriptor> bind(int startIndex, Object value) {
            return type.bind(startIndex, schema, value);
        }

        @Override
        public String toString() {
            return "FieldDescriptor{" +
                    "schema=" + schema +
                    ", name='" + name + '\'' +
                    ", key=" + isKey +
                    ", typeName='" + typeName + '\'' +
                    ", type=" + type +
                    ", columnName='" + columnName + '\'' +
                    '}';
        }
    }

}
