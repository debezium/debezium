/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.debezium.document.Array;
import io.debezium.document.Array.Entry;
import io.debezium.document.Document;
import io.debezium.document.Value;
import io.debezium.relational.Attribute;
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
        if (tableChange.getPreviousId() != null) {
            document.setString("previousId", tableChange.getPreviousId().toDoubleQuotedString());
        }

        if (tableChange.getTable() != null) {
            document.setDocument("table", toDocument(tableChange.getTable()));
            document.setString("comment", tableChange.getTable().comment());
        }

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

        List<Document> attributes = table.attributes()
                .stream()
                .map(this::toDocument)
                .collect(Collectors.toList());

        document.setArray("attributes", Array.create(attributes));

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
        document.setString("comment", column.comment());
        document.setBoolean("hasDefaultValue", column.hasDefaultValue());

        column.defaultValueExpression().ifPresent(d -> document.setString("defaultValueExpression", d));

        Optional.ofNullable(column.enumValues())
                .map(List::toArray)
                .ifPresent(enums -> document.setArray("enumValues", enums));

        return document;
    }

    private Document toDocument(Attribute attribute) {
        final Document document = Document.create();
        document.setString("name", attribute.name());
        document.setString("value", attribute.value());
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
                tableChanges.alter(change);
            }
            else if (change.getType() == TableChangeType.DROP) {
                tableChanges.drop(change.getId());
            }
        }

        return tableChanges;
    }

    private static Table fromDocument(TableId id, Document document) {
        TableEditor editor = Table.editor()
                .tableId(id)
                .setDefaultCharsetName(document.getString("defaultCharsetName"));
        if (document.getString("comment") != null) {
            editor.setComment(document.getString("comment"));
        }

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

                    String columnComment = v.getString("comment");
                    if (columnComment != null) {
                        columnEditor.comment(columnComment);
                    }

                    Boolean hasDefaultValue = v.getBoolean("hasDefaultValue");
                    String defaultValueExpression = v.getString("defaultValueExpression");
                    if (defaultValueExpression != null) {
                        columnEditor.defaultValueExpression(defaultValueExpression);
                    }
                    else if (Boolean.TRUE.equals(hasDefaultValue)) {
                        columnEditor.defaultValueExpression(null);
                    }

                    Array enumValues = v.getArray("enumValues");
                    if (enumValues != null && !enumValues.isEmpty()) {
                        List<String> enumValueList = enumValues.streamValues()
                                .map(Value::asString)
                                .collect(Collectors.toList());
                        columnEditor.enumValues(enumValueList);
                    }

                    columnEditor.position(v.getInteger("position"))
                            .optional(v.getBoolean("optional"))
                            .autoIncremented(v.getBoolean("autoIncremented"))
                            .generated(v.getBoolean("generated"));

                    return columnEditor.create();
                })
                .forEach(editor::addColumn);

        document.getOrCreateArray("attributes")
                .streamValues()
                .map(Value::asDocument)
                .map(v -> Attribute.editor().name(v.getString("name")).value(v.getString("value")).create())
                .forEach(editor::addAttribute);

        editor.setPrimaryKeyNames(document.getArray("primaryKeyColumnNames")
                .streamValues()
                .map(Value::asString)
                .collect(Collectors.toList()));

        return editor.create();
    }

    public static TableChange fromDocument(Document document, boolean useCatalogBeforeSchema) {
        TableChangeType type = TableChangeType.valueOf(document.getString("type"));
        TableId id = TableId.parse(document.getString("id"), useCatalogBeforeSchema);
        TableId previousId = null;
        if (document.has("previousId")) {
            previousId = TableId.parse(document.getString("previousId"), useCatalogBeforeSchema);
        }
        Table table = null;

        if (type == TableChangeType.CREATE || type == TableChangeType.ALTER) {
            table = fromDocument(id, document.getDocument("table"));
        }
        else {
            table = Table.editor().tableId(id).create();
        }
        return new TableChange(type, table, previousId);
    }
}
