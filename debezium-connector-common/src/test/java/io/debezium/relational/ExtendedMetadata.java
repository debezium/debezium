/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Types;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.document.Document;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Interner;

/**
 * Sample connector metadata types for tests: a {@link Column} that carries a region, a {@link Table} that
 * carries a label, the editors that set them, the serializers that persist them under a nested
 * {@value #EXTENDED_KEY} entry, and a {@link RelationalMetadataFactory} that ties them together.
 * They follow the shape a connector such as Oracle uses to extend the core metadata classes.
 *
 * @author Chris Cranford
 */
public final class ExtendedMetadata {

    /** The nested entry both serializers write their fields under, keeping the base layout flat. */
    public static final String EXTENDED_KEY = "extended";
    public static final String REGION_KEY = "region";
    public static final String LABEL_KEY = "label";

    private ExtendedMetadata() {
    }

    public static ExtendedColumn column(String name, String region) {
        final ExtendedColumnEditor editor = ExtendedRelationalMetadataFactory.INSTANCE.columnEditor();
        editor.name(name).type("NUMBER").jdbcType(Types.NUMERIC).optional(false);
        return editor.region(region).create();
    }

    public static ExtendedTable table(TableId id, String label, String region) {
        final ExtendedTableEditor editor = ExtendedRelationalMetadataFactory.INSTANCE.tableEditor();
        editor.tableId(id).addColumn(column("id", region)).setPrimaryKeyNames("id");
        return editor.label(label).create();
    }

    public interface ExtendedColumn extends Column {
        String region();

        @Override
        ExtendedColumnEditor edit();
    }

    public interface ExtendedColumnEditor extends ColumnEditor {
        String region();

        ExtendedColumnEditor region(String region);

        @Override
        ExtendedColumn create();
    }

    public static final class ExtendedColumnImpl extends ColumnImpl implements ExtendedColumn {
        private final String region;

        ExtendedColumnImpl(ExtendedColumnEditor editor) {
            super(editor);
            this.region = editor.region();
        }

        @Override
        public String region() {
            return region;
        }

        @Override
        public ExtendedColumnEditor edit() {
            return copyTo(new ExtendedColumnEditorImpl()).region(region);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ExtendedColumn that && super.equals(that) && Objects.equals(region, that.region());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), region);
        }

        @Override
        protected void appendExtensions(StringBuilder sb) {
            if (region != null) {
                sb.append(" REGION ").append(region);
            }
        }
    }

    public static final class ExtendedColumnEditorImpl extends ColumnEditorImpl implements ExtendedColumnEditor {
        private String region;

        @Override
        public String region() {
            return region;
        }

        @Override
        public ExtendedColumnEditor region(String region) {
            this.region = region;
            return this;
        }

        @Override
        public ExtendedColumn create() {
            return Interner.intern(new ExtendedColumnImpl(this));
        }
    }

    public interface ExtendedTable extends Table {
        String label();

        @Override
        ExtendedTableEditor edit();
    }

    public interface ExtendedTableEditor extends TableEditor {
        String label();

        ExtendedTableEditor label(String label);

        @Override
        ExtendedTable create();
    }

    public static final class ExtendedTableImpl extends TableImpl implements ExtendedTable {
        private final String label;

        ExtendedTableImpl(ExtendedTableEditor editor) {
            super(editor);
            this.label = editor.label();
        }

        @Override
        public String label() {
            return label;
        }

        @Override
        public ExtendedTableEditor edit() {
            return copyTo(new ExtendedTableEditorImpl()).label(label);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ExtendedTable that && super.equals(that) && Objects.equals(label, that.label());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), label);
        }

        @Override
        protected void appendExtensions(StringBuilder sb, String prefix) {
            if (label != null) {
                sb.append(prefix).append("label: ").append(label).append(System.lineSeparator());
            }
        }
    }

    public static final class ExtendedTableEditorImpl extends TableEditorImpl implements ExtendedTableEditor {
        private String label;

        @Override
        public String label() {
            return label;
        }

        @Override
        public ExtendedTableEditor label(String label) {
            this.label = label;
            return this;
        }

        @Override
        public ExtendedTable create() {
            checkCreatable();
            return Interner.intern(new ExtendedTableImpl(this));
        }
    }

    /**
     * Persists the extension fields in schema history as a nested {@value #EXTENDED_KEY} document and reads
     * them back; a document without that entry leaves the editor defaults in place.
     */
    public static class ExtendedJsonTableChangeSerializer extends JsonTableChangeSerializer {

        public ExtendedJsonTableChangeSerializer(RelationalMetadataFactory metadataFactory) {
            super(metadataFactory);
        }

        @Override
        protected void writeColumnExtensions(Column column, Document document) {
            final String region = column.unwrap(ExtendedColumn.class).region();
            if (region != null) {
                document.setDocument(EXTENDED_KEY).setString(REGION_KEY, region);
            }
        }

        @Override
        protected void readColumnExtensions(Document document, ColumnEditor editor) {
            final Document extended = document.getDocument(EXTENDED_KEY);
            if (extended != null) {
                editor.unwrap(ExtendedColumnEditor.class).region(extended.getString(REGION_KEY));
            }
        }

        @Override
        protected void writeTableExtensions(Table table, Document document) {
            final String label = table.unwrap(ExtendedTable.class).label();
            if (label != null) {
                document.setDocument(EXTENDED_KEY).setString(LABEL_KEY, label);
            }
        }

        @Override
        protected void readTableExtensions(Document document, TableEditor editor) {
            final Document extended = document.getDocument(EXTENDED_KEY);
            if (extended != null) {
                editor.unwrap(ExtendedTableEditor.class).label(extended.getString(LABEL_KEY));
            }
        }
    }

    /**
     * Declares the nested {@value #EXTENDED_KEY} struct on the schema change column and table schemas.
     */
    public static final class ExtendedSchemaFactory extends SchemaFactory {

        public static final ExtendedSchemaFactory INSTANCE = new ExtendedSchemaFactory();

        public Schema extendedColumnSchema() {
            return SchemaBuilder.struct()
                    .name("io.debezium.test.ExtendedColumn")
                    .field(REGION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();
        }

        public Schema extendedTableSchema() {
            return SchemaBuilder.struct()
                    .name("io.debezium.test.ExtendedTable")
                    .field(LABEL_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();
        }

        @Override
        public SchemaBuilder schemaHistoryColumnSchemaBuilder(SchemaNameAdjuster adjuster) {
            return super.schemaHistoryColumnSchemaBuilder(adjuster).field(EXTENDED_KEY, extendedColumnSchema());
        }

        @Override
        public SchemaBuilder schemaHistoryTableSchemaBuilder(SchemaNameAdjuster adjuster) {
            return super.schemaHistoryTableSchemaBuilder(adjuster).field(EXTENDED_KEY, extendedTableSchema());
        }
    }

    /**
     * Writes the extension fields into the nested {@value #EXTENDED_KEY} struct of schema change events.
     */
    public static class ExtendedConnectTableChangeSerializer extends ConnectTableChangeSerializer {

        public ExtendedConnectTableChangeSerializer(SchemaNameAdjuster adjuster) {
            super(adjuster, ExtendedSchemaFactory.INSTANCE);
        }

        @Override
        protected void writeColumnExtensions(Column column, Struct struct) {
            final String region = column.unwrap(ExtendedColumn.class).region();
            if (region != null) {
                final Struct extended = new Struct(struct.schema().field(EXTENDED_KEY).schema());
                extended.put(REGION_KEY, region);
                struct.put(EXTENDED_KEY, extended);
            }
        }

        @Override
        protected void writeTableExtensions(Table table, Struct struct) {
            final String label = table.unwrap(ExtendedTable.class).label();
            if (label != null) {
                final Struct extended = new Struct(struct.schema().field(EXTENDED_KEY).schema());
                extended.put(LABEL_KEY, label);
                struct.put(EXTENDED_KEY, extended);
            }
        }
    }

    /**
     * The factory a connector returns from {@code getRelationalMetadataFactory()} to have every column
     * and table created by the connector carry the extension fields, and every serializer persist them.
     */
    public static final class ExtendedRelationalMetadataFactory implements RelationalMetadataFactory {

        public static final ExtendedRelationalMetadataFactory INSTANCE = new ExtendedRelationalMetadataFactory();

        private ExtendedRelationalMetadataFactory() {
        }

        @Override
        public ExtendedColumnEditor columnEditor() {
            return new ExtendedColumnEditorImpl();
        }

        @Override
        public ExtendedTableEditor tableEditor() {
            return new ExtendedTableEditorImpl();
        }

        @Override
        public JsonTableChangeSerializer jsonTableChangesSerializer() {
            return new ExtendedJsonTableChangeSerializer(this);
        }

        @Override
        public ConnectTableChangeSerializer connectTableChangesSerializer(SchemaNameAdjuster adjuster) {
            return new ExtendedConnectTableChangeSerializer(adjuster);
        }
    }
}
