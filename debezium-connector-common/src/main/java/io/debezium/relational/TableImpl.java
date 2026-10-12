/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.PackagePrivate;
import io.debezium.util.Interner;
import io.debezium.util.Strings;

/**
 * The default {@link Table} implementation.
 * <p>
 * A connector that needs to carry additional table metadata subclasses this class, adds its own fields,
 * and creates instances from its own {@link TableEditorImpl} subclass. Such a subclass must override
 * {@link #equals(Object)} to accept only instances of its own type and compare its own fields after the base
 * fields, because {@link Tables} and {@link Interner} rely on equality to detect a changed table and an
 * equal TableImpl must never stand in for the subclass. It must override {@link #hashCode()} to combine the base
 * hash code with the same fields, and should override {@link #edit()} and {@link #appendExtensions(StringBuilder, String)}.
 * The extra fields add metadata and never change identity, which stays with the table identifier.
 */
public class TableImpl implements Table {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableImpl.class);

    private final TableId id;
    private final List<Column> columnDefs;
    private final List<String> pkColumnNames;
    private final Map<String, Column> columnsByName;
    private final String defaultCharsetName;
    private final String comment;
    private final List<Attribute> attributes;

    @PackagePrivate
    TableImpl(Table table) {
        this(table.id(), table.columns(), table.primaryKeyColumnNames(), table.defaultCharsetName(), table.comment(), table.attributes());
    }

    /**
     * Creates a table from the current state of the given editor, applying the editor's default character set
     * to every column. Subclasses call this constructor and then copy their own fields from their own editor type.
     *
     * @param editor the editor whose state is copied; may not be null
     */
    protected TableImpl(TableEditor editor) {
        this(editor.tableId(), columnsWithTableCharset(editor), editor.primaryKeyColumnNames(), editor.defaultCharsetName(),
                editor.comment(), editor.attributes());
    }

    protected TableImpl(TableId id, List<Column> sortedColumns, List<String> pkColumnNames, String defaultCharsetName, String comment,
                        List<Attribute> attributes) {
        this.id = Interner.intern(id);
        this.columnDefs = Interner.intern(Collections.unmodifiableList(sortedColumns));
        this.pkColumnNames = pkColumnNames == null ? Collections.emptyList() : Interner.intern(Collections.unmodifiableList(pkColumnNames));
        Map<String, Column> defsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column def : this.columnDefs) {
            final Column shadowed = defsByName.put(Interner.intern(def.name()), def);
            if (shadowed != null) {
                LOGGER.warn("Table '{}' defines columns '{}' and '{}' whose names differ only by case; lookups by column name will resolve to '{}'",
                        this.id, shadowed.name(), def.name(), def.name());
            }
        }
        this.columnsByName = Interner.intern(Collections.unmodifiableMap(defsByName));
        this.defaultCharsetName = Interner.intern(defaultCharsetName);
        this.comment = Interner.intern(comment);
        this.attributes = attributes == null ? null : Interner.intern(Collections.unmodifiableList(attributes));
    }

    private static List<Column> columnsWithTableCharset(TableEditor editor) {
        final String defaultCharsetName = editor.defaultCharsetName();
        final List<Column> columns = new ArrayList<>();
        for (Column column : editor.columns()) {
            columns.add(column.edit().charsetNameOfTable(defaultCharsetName).create());
        }
        return columns;
    }

    @Override
    public TableId id() {
        return id;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return pkColumnNames;
    }

    @Override
    public List<Column> columns() {
        return columnDefs;
    }

    @Override
    public List<String> retrieveColumnNames() {
        return columnDefs.stream()
                .map(Column::name)
                .collect(Collectors.toList());
    }

    @Override
    public Column columnWithName(String name) {
        return columnsByName.get(name);
    }

    @Override
    public String defaultCharsetName() {
        return defaultCharsetName;
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public List<Attribute> attributes() {
        return attributes;
    }

    @Override
    public Attribute attributeWithName(String name) {
        if (attributes == null) {
            return null;
        }
        return attributes.stream().filter(a -> name.equalsIgnoreCase(a.name())).findFirst().orElse(null);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Table) {
            Table that = (Table) obj;
            return this.id().equals(that.id())
                    && this.columns().equals(that.columns())
                    && this.primaryKeyColumnNames().equals(that.primaryKeyColumnNames())
                    && Strings.equalsIgnoreCase(this.defaultCharsetName(), that.defaultCharsetName());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, "");
        return sb.toString();
    }

    public void toString(StringBuilder sb, String prefix) {
        if (prefix == null) {
            prefix = "";
        }
        sb.append(prefix).append("columns: {").append(System.lineSeparator());
        for (Column defn : columnDefs) {
            sb.append(prefix).append("  ").append(defn).append(System.lineSeparator());
        }
        sb.append(prefix).append("}").append(System.lineSeparator());
        sb.append(prefix).append("primary key: ").append(primaryKeyColumnNames()).append(System.lineSeparator());
        sb.append(prefix).append("default charset: ").append(defaultCharsetName()).append(System.lineSeparator());
        sb.append(prefix).append("comment: ").append(comment()).append(System.lineSeparator());
        sb.append(prefix).append("attributes: {").append(System.lineSeparator());
        if (attributes != null) {
            for (Attribute attribute : attributes) {
                sb.append(prefix).append("  ").append(attribute).append(System.lineSeparator());
            }
        }
        sb.append(prefix).append("}").append(System.lineSeparator());
        appendExtensions(sb, prefix);
    }

    /**
     * Appends any subclass-specific state to the {@link #toString()} output. The default appends nothing.
     *
     * @param sb the builder holding the base table rendering; never null
     * @param prefix the line prefix in use; never null
     */
    protected void appendExtensions(StringBuilder sb, String prefix) {
    }

    @Override
    public TableEditor edit() {
        return copyTo(new TableEditorImpl());
    }

    /**
     * Copies the state of this table into the given editor and returns that editor. Subclasses call this
     * from their own {@link #edit()} with their own editor type and then set their additional fields.
     *
     * @param editor the editor to populate; may not be null
     * @return the given editor
     * @param <E> the editor type
     */
    protected <E extends TableEditor> E copyTo(E editor) {
        editor.tableId(id)
                .setColumns(columnDefs)
                .setPrimaryKeyNames(pkColumnNames)
                .setDefaultCharsetName(defaultCharsetName)
                .setComment(comment);
        if (attributes != null) {
            editor.addAttributes(attributes);
        }
        return editor;
    }
}
