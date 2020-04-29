/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.debezium.document.Array;
import io.debezium.document.Array.Entry;
import io.debezium.document.Document;
import io.debezium.document.Value;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;

/**
 * Ther serializer responsible for converting of {@link TableChanges} into a JSON format.
 *
 * @author Jiri Pechanec
 *
 */
public class JsonTableChangeSerializer implements TableChanges.TableChangesSerializer<Array> {

    @Override
    public Array serialize(TableChanges tableChanges) {
        List<Value> values = StreamSupport.stream(tableChanges.spliterator(), false)
                .map(this::toDocument)
                .map(Value::create)
                .collect(Collectors.toList());

        return Array.create(values);
    }

    public Document toDocument(TableChange tableChange) {
        Document document = Document.create();

        document.setString("type", tableChange.getType().name());
        document.setString("id", tableChange.getId().toDoubleQuotedString());
        document.setDocument("table", toDocument(tableChange.getTable()));
        return document;
    }

    private Document toDocument(Table table) {
        Document document = Document.create();

        document.set("defaultCharsetName", table.defaultCharsetName());
        document.set("primaryKeyColumnNames", Array.create(table.primaryKeyColumnNames()));

        List<Document> columns = table.columns()
                .stream()
                .map(this::toDocument)
                .collect(Collectors.toList());

        document.setArray("columns", Array.create(columns));

        return document;
    }

    private Document toDocument(Column column) {
        Document document = Document.create();

        document.setString("name", column.name());
        document.setNumber("jdbcType", column.jdbcType());

        if (column.nativeType() != Column.UNSET_INT_VALUE) {
            document.setNumber("nativeType", column.nativeType());
        }

        document.setString("typeName", column.typeName());
        document.setString("typeExpression", column.typeExpression());
        document.setString("charsetName", column.charsetName());

        if (column.length() != Column.UNSET_INT_VALUE) {
            document.setNumber("length", column.length());
        }

        column.scale().ifPresent(s -> document.setNumber("scale", s));

        document.setNumber("position", column.position());
        document.setBoolean("optional", column.isOptional());
        document.setBoolean("autoIncremented", column.isAutoIncremented());
        document.setBoolean("generated", column.isGenerated());

        return document;
    }

    @Override
    public TableChanges deserialize(Array array, boolean useCatalogBeforeSchema) {
        TableChanges tableChanges = new TableChanges();

        for (Entry entry : array) {
            TableChange change = fromDocument(entry.getValue().asDocument(), useCatalogBeforeSchema);

            if (change.getType() == TableChangeType.CREATE) {
                tableChanges.create(change.getTable());
            }
            else if (change.getType() == TableChangeType.ALTER) {
                tableChanges.alter(change.getTable());
            }
        }

        return tableChanges;
    }

    private static Table fromDocument(TableId id, Document document) {
        TableEditor editor = Table.editor()
                .tableId(id)
                .setDefaultCharsetName(document.getString("defaultCharsetName"));

        document.getArray("columns")
                .streamValues()
                .map(Value::asDocument)
                .map(v -> {
                    ColumnEditor columnEditor = Column.editor()
                            .name(v.getString("name"))
                            .jdbcType(v.getInteger("jdbcType"));

                    Integer nativeType = v.getInteger("nativeType");
                    if (nativeType != null) {
                        columnEditor.nativeType(nativeType);
                    }

                    columnEditor.type(v.getString("typeName"), v.getString("typeExpression"))
                            .charsetName(v.getString("charsetName"));

                    Integer length = v.getInteger("length");
                    if (length != null) {
                        columnEditor.length(length);
                    }

                    Integer scale = v.getInteger("scale");
                    if (scale != null) {
                        columnEditor.scale(scale);
                    }

                    columnEditor.position(v.getInteger("position"))
                            .optional(v.getBoolean("optional"))
                            .autoIncremented(v.getBoolean("autoIncremented"))
                            .generated(v.getBoolean("generated"));

                    return columnEditor.create();
                })
                .forEach(editor::addColumn);

        editor.setPrimaryKeyNames(document.getArray("primaryKeyColumnNames")
                .streamValues()
                .map(Value::asString)
                .collect(Collectors.toList()));

        return editor.create();
    }

    public static TableChange fromDocument(Document document, boolean useCatalogBeforeSchema) {
        TableChangeType type = TableChangeType.valueOf(document.getString("type"));
        TableId id = TableId.parse(document.getString("id"), useCatalogBeforeSchema);
        Table table = null;

        if (type == TableChangeType.CREATE || type == TableChangeType.ALTER) {
            table = fromDocument(id, document.getDocument("table"));
        }

        return new TableChange(type, table);
    }
}
