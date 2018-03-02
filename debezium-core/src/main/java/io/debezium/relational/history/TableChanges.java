/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.Value;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class TableChanges {

    private final List<TableChange> changes;

    public TableChanges() {
        this.changes = new ArrayList<>();
    }

    public TableChanges create(Table table) {
        changes.add(new TableChange(TableChangeType.CREATE, table));
        return this;
    }

    public Array toArray() {
        List<Value> values = changes.stream()
            .map(TableChange::toDocument)
            .map(Value::create)
            .collect(Collectors.toList());

        return Array.create(values);
    }
    public static class TableChange {

        private final TableChangeType type;
        private final TableId id;
        private final Table table;

        public TableChange(TableChangeType type, Table table) {
            this.type = type;
            this.table = table;
            this.id = table.id();
        }

        public Document toDocument() {
            Document document = Document.create();

            document.setString("type", type.name());
            document.setString("id", id.toString());
            document.setDocument("table", toDocument(table));
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

            if (column.scale() != Column.UNSET_INT_VALUE) {
                document.setNumber("scale", column.scale());
            }

            document.setNumber("position", column.position());
            document.setBoolean("optional", column.isOptional());
            document.setBoolean("autoIncremented", column.isAutoIncremented());
            document.setBoolean("generated", column.isGenerated());

            return document;
        }
    }

    public enum TableChangeType {
        CREATE,
        ALTER,
        DROP;
    }
}
