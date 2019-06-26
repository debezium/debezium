/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;

import java.util.List;
import java.util.Objects;

import static io.debezium.connector.cassandra.SchemaHolder.getFieldSchema;

/**
 * An immutable data structure representing a change event, and can be converted
 * to a GenericRecord representing key/value of the change event.
 */
public abstract class Record implements Event {
    static final String NAMESPACE = "io.debezium.connector.cassandra.data";
    static final String AFTER = "after";
    static final String OPERATION = "op";
    static final String SOURCE = "source";
    static final String TIMESTAMP = "ts_ms";

    @SuppressWarnings("squid:S1845")
    private final SourceInfo source;
    private final RowData rowData;
    private final Operation op;
    private final long ts;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final boolean shouldMarkOffset;

    public enum Operation {
        INSERT("i"),
        UPDATE("u"),
        DELETE("d");

        private String value;

        Operation(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    Record(SourceInfo source, RowData rowData, Schema keySchema, Schema valueSchema, Operation op, boolean shouldMarkOffset, long ts) {
        this.source = source;
        this.rowData = rowData;
        this.op = op;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.shouldMarkOffset = shouldMarkOffset;
        this.ts = ts;
    }

    public GenericRecord buildKey() {
        if (keySchema == null) {
            return null;
        }

        List<CellData> primary = rowData.getPrimary();
        GenericRecordBuilder builder = new GenericRecordBuilder(keySchema);
        for (CellData cellData : primary) {
            builder.set(cellData.name, cellData.value);
        }
        return builder.build();
    }

    public GenericRecord buildValue() {
        if (valueSchema == null) {
            return null;
        }

        return new GenericRecordBuilder(valueSchema)
                .set(TIMESTAMP, ts)
                .set(OPERATION, op.getValue())
                .set(SOURCE, source.record(getFieldSchema(SOURCE, valueSchema)))
                .set(AFTER, rowData.record(getFieldSchema(AFTER, valueSchema)))
                .build();
    }

    public static Schema keySchema(TableMetadata tm) {
        if (tm == null) {
            return null;
        }
        SchemaBuilder.FieldAssembler assembler = SchemaBuilder.builder().record(getKeyName(tm)).namespace(NAMESPACE).fields();
        for (ColumnMetadata cm : tm.getPrimaryKey()) {
            AbstractType<?> convertedType = CassandraTypeConverter.convert(cm.getType());
            Schema colSchema = CassandraTypeToAvroSchemaMapper.getSchema(convertedType, false);
            if (colSchema != null) {
                assembler.name(cm.getName()).type(colSchema).noDefault();
            }
        }
        return (Schema) assembler.endRecord();
    }

    public static Schema valueSchema(TableMetadata tm) {
        if (tm == null) {
            return null;
        }
        return SchemaBuilder.builder().record(getValueName(tm)).namespace(NAMESPACE).fields()
                .name(TIMESTAMP).type().longType().noDefault()
                .name(OPERATION).type().stringType().noDefault()
                .name(SOURCE).type(SourceInfo.SOURCE_SCHEMA).noDefault()
                .name(AFTER).type(RowData.rowSchema(tm)).noDefault()
                .endRecord();
    }

    @Override
    public String toString() {
        return "Record{"
                + "source=" + source
                + ", after=" + rowData
                + ", keySchema=" + keySchema
                + ", valueSchema=" + valueSchema
                + ", op=" + op
                + ", ts=" + ts
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Record record = (Record) o;
        return ts == record.ts
                && Objects.equals(source, record.source)
                && Objects.equals(rowData, record.rowData)
                && Objects.equals(keySchema, record.keySchema)
                && Objects.equals(valueSchema, record.valueSchema)
                && op == record.op;
    }

    public static String getKeyName(TableMetadata tm) {
        return DatabaseDescriptor.getClusterName() + "." + tm.getKeyspace().getName() + "." + tm.getName() + ".Key";
    }

    public static String getValueName(TableMetadata tm) {
        return DatabaseDescriptor.getClusterName() + "." + tm.getKeyspace().getName() + "." + tm.getName() + ".Value";
    }


    @Override
    public int hashCode() {
        return Objects.hash(source, rowData, keySchema, valueSchema, op, ts);
    }

    public SourceInfo getSource() {
        return source;
    }

    public RowData getRowData() {
        return rowData;
    }

    public Operation getOp() {
        return op;
    }

    public long getTs() {
        return ts;
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public boolean shouldMarkOffset() {
        return shouldMarkOffset;
    }
}
