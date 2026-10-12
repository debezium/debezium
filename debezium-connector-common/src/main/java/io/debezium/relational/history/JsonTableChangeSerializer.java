/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.List;
import java.util.Objects;
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
import io.debezium.relational.DefaultRelationalMetadataFactory;
import io.debezium.relational.RelationalMetadataFactory;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;

/**
 * The serializer responsible for converting of {@link TableChanges} into a JSON format.
 * <p>
 * Tables and columns read back from JSON are created through the {@link RelationalMetadataFactory} the
 * serializer was built with. A connector whose metadata classes carry additional fields subclasses this
 * serializer and overrides the extension hooks ({@link #writeColumnExtensions}, {@link #readColumnExtensions},
 * {@link #writeTableExtensions}, {@link #readTableExtensions}); the base fields and their keys never change.
 *
 * @author Jiri Pechanec
 *
 */
public class JsonTableChangeSerializer implements TableChanges.TableChangesSerializer<Array> {

    private final RelationalMetadataFactory metadataFactory;

    public JsonTableChangeSerializer() {
        this(DefaultRelationalMetadataFactory.INSTANCE);
    }

    public JsonTableChangeSerializer(RelationalMetadataFactory metadataFactory) {
        this.metadataFactory = Objects.requireNonNull(metadataFactory, "metadataFactory");
    }

    protected RelationalMetadataFactory metadataFactory() {
        return metadataFactory;
    }

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
        }

        return document;
    }

    /**
     * Converts a single table to its document form, the same form used inside a table change document.
     *
     * @param table the table; never null
     * @return the document; never null
     */
    public Document toDocument(Table table) {
        Document document = Document.create();

        document.set("defaultCharsetName", table.defaultCharsetName());
        document.setString("comment", table.comment());
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

        writeTableExtensions(table, document);
        return document;
    }

    protected Document toDocument(Column column) {
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

        writeColumnExtensions(column, document);
        return document;
    }

    /**
     * Writes connector-specific column metadata into the column's document. The default writes nothing.
     * Implementations should nest their fields under a single entry keyed by the connector name so the base
     * layout stays flat.
     *
     * @param column the column being serialized; never null
     * @param document the column's document, already holding the base fields; never null
     */
    protected void writeColumnExtensions(Column column, Document document) {
    }

    /**
     * Reads connector-specific column metadata written by {@link #writeColumnExtensions} back into the editor.
     * The default reads nothing. A document written before the extension existed has no entry, and
     * implementations should leave the editor's defaults in place in that case.
     *
     * @param document the column's document; never null
     * @param editor the editor created by the metadata factory, already holding the base fields; never null
     */
    protected void readColumnExtensions(Document document, ColumnEditor editor) {
    }

    /**
     * Writes connector-specific table metadata into the table's document. The default writes nothing.
     *
     * @param table the table being serialized; never null
     * @param document the table's document, already holding the base fields; never null
     */
    protected void writeTableExtensions(Table table, Document document) {
    }

    /**
     * Reads connector-specific table metadata written by {@link #writeTableExtensions} back into the editor.
     * The default reads nothing.
     *
     * @param document the table's document; never null
     * @param editor the editor created by the metadata factory, already holding the base fields; never null
     */
    protected void readTableExtensions(Document document, TableEditor editor) {
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

    /**
     * Rebuilds a table from the document form produced by {@link #toDocument(Table)}, using the metadata factory
     * this serializer was created with.
     *
     * @param id the table identifier; never null
     * @param document the table document; never null
     * @return the table; never null
     */
    public Table fromDocument(TableId id, Document document) {
        TableEditor editor = metadataFactory.tableEditor()
                .tableId(id)
                .setDefaultCharsetName(document.getString("defaultCharsetName"));
        if (document.getString("comment") != null) {
            editor.setComment(document.getString("comment"));
        }

        document.getArray("columns")
                .streamValues()
                .map(Value::asDocument)
                .map(v -> {
                    ColumnEditor columnEditor = metadataFactory.columnEditor()
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

                    readColumnExtensions(v, columnEditor);
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

        readTableExtensions(document, editor);
        return editor.create();
    }

    public TableChange fromDocument(Document document, boolean useCatalogBeforeSchema) {
        TableChangeType type = TableChangeType.valueOf(document.getString("type"));
        TableId id = TableId.parse(document.getString("id"), useCatalogBeforeSchema);
        TableId previousId = null;
        if (document.has("previousId")) {
            previousId = TableId.parse(document.getString("previousId"), useCatalogBeforeSchema);
        }
        Table table = null;

        if (type == TableChangeType.CREATE || type == TableChangeType.ALTER) {
            Document tableDocument = document.getDocument("table");
            // Records written by earlier versions carry the table comment on the change document only
            if (!tableDocument.has("comment") && document.getString("comment") != null) {
                tableDocument.setString("comment", document.getString("comment"));
            }
            table = fromDocument(id, tableDocument);
        }
        else {
            table = metadataFactory.tableEditor().tableId(id).create();
        }
        return new TableChange(type, table, previousId);
    }
}
