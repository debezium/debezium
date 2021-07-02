/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Ther serializer responsible for converting of {@link TableChanges} into an array of {@link Struct}s.
 *
 * @author Jiri Pechanec
 *
 */
public class ConnectTableChangeSerializer implements TableChanges.TableChangesSerializer<List<Struct>> {

    public static final String ID_KEY = "id";
    public static final String TYPE_KEY = "type";
    public static final String TABLE_KEY = "table";
    public static final String DEFAULT_CHARSET_NAME_KEY = "defaultCharsetName";
    public static final String PRIMARY_KEY_COLUMN_NAMES_KEY = "primaryKeyColumnNames";
    public static final String COLUMNS_KEY = "columns";
    public static final String NAME_KEY = "name";
    public static final String JDBC_TYPE_KEY = "jdbcType";
    public static final String NATIVE_TYPE_KEY = "nativeType";
    public static final String TYPE_NAME_KEY = "typeName";
    public static final String TYPE_EXPRESSION_KEY = "typeExpression";
    public static final String CHARSET_NAME_KEY = "charsetName";
    public static final String LENGTH_KEY = "length";
    public static final String SCALE_KEY = "scale";
    public static final String POSITION_KEY = "position";
    public static final String OPTIONAL_KEY = "optional";
    public static final String AUTO_INCREMENTED_KEY = "autoIncremented";
    public static final String GENERATED_KEY = "generated";
    public static final String COMMENT_KEY = "comment";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectTableChangeSerializer.class);
    private static final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

    private static final Schema COLUMN_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Column"))
            .field(NAME_KEY, Schema.STRING_SCHEMA)
            .field(JDBC_TYPE_KEY, Schema.INT32_SCHEMA)
            .field(NATIVE_TYPE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
            .field(TYPE_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TYPE_EXPRESSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(LENGTH_KEY, Schema.OPTIONAL_INT32_SCHEMA)
            .field(SCALE_KEY, Schema.OPTIONAL_INT32_SCHEMA)
            .field(POSITION_KEY, Schema.INT32_SCHEMA)
            .field(OPTIONAL_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(AUTO_INCREMENTED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(GENERATED_KEY, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema TABLE_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Table"))
            .field(DEFAULT_CHARSET_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PRIMARY_KEY_COLUMN_NAMES_KEY, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field(COLUMNS_KEY, SchemaBuilder.array(COLUMN_SCHEMA).build())
            .field(COMMENT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    public static final Schema CHANGE_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.schema.Change"))
            .field(TYPE_KEY, Schema.STRING_SCHEMA)
            .field(ID_KEY, Schema.STRING_SCHEMA)
            .field(TABLE_KEY, TABLE_SCHEMA)
            .build();

    @Override
    public List<Struct> serialize(TableChanges tableChanges) {
        return StreamSupport.stream(tableChanges.spliterator(), false)
                .map(this::toStruct)
                .collect(Collectors.toList());
    }

    public Struct toStruct(TableChange tableChange) {
        final Struct struct = new Struct(CHANGE_SCHEMA);

        struct.put(TYPE_KEY, tableChange.getType().name());
        struct.put(ID_KEY, tableChange.getId().toDoubleQuotedString());
        struct.put(TABLE_KEY, toStruct(tableChange.getTable()));
        return struct;
    }

    private Struct toStruct(Table table) {
        final Struct struct = new Struct(TABLE_SCHEMA);

        struct.put(DEFAULT_CHARSET_NAME_KEY, table.defaultCharsetName());
        struct.put(PRIMARY_KEY_COLUMN_NAMES_KEY, table.primaryKeyColumnNames());

        final List<Struct> columns = table.columns().stream()
                .map(this::toStruct)
                .collect(Collectors.toList());

        struct.put(COLUMNS_KEY, columns);
        struct.put(COMMENT_KEY, table.comment());
        return struct;
    }

    private Struct toStruct(Column column) {
        final Struct struct = new Struct(COLUMN_SCHEMA);

        struct.put(NAME_KEY, column.name());
        struct.put(JDBC_TYPE_KEY, column.jdbcType());

        if (column.nativeType() != Column.UNSET_INT_VALUE) {
            struct.put(NATIVE_TYPE_KEY, column.nativeType());
        }

        struct.put(TYPE_NAME_KEY, column.typeName());
        struct.put(TYPE_EXPRESSION_KEY, column.typeExpression());
        struct.put(CHARSET_NAME_KEY, column.charsetName());

        if (column.length() != Column.UNSET_INT_VALUE) {
            struct.put(LENGTH_KEY, column.length());
        }

        column.scale().ifPresent(s -> struct.put(SCALE_KEY, s));

        struct.put(POSITION_KEY, column.position());
        struct.put(OPTIONAL_KEY, column.isOptional());
        struct.put(AUTO_INCREMENTED_KEY, column.isAutoIncremented());
        struct.put(GENERATED_KEY, column.isGenerated());
        struct.put(COMMENT_KEY, column.comment());

        return struct;
    }

    @Override
    public TableChanges deserialize(List<Struct> data, boolean useCatalogBeforeSchema) {
        throw new UnsupportedOperationException("Deserialization from Connect Struct is not supported");
    }
}
