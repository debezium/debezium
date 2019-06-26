/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.debezium.connector.cassandra.Record.AFTER;
import static io.debezium.connector.cassandra.SchemaHolder.getFieldSchema;

/**
 * Row-level data about the source event. Contains a map where the key is the table column
 * name and the value is the {@link CellData}.
 */
public class RowData implements AvroRecord {
    private final Map<String, CellData> cellMap = new LinkedHashMap<>();

    public void addCell(CellData cellData) {
        this.cellMap.put(cellData.name, cellData);
    }

    public void removeCell(String columnName) {
        if (hasCell(columnName)) {
            cellMap.remove(columnName);
        }
    }

    public boolean hasCell(String columnName) {
        return cellMap.containsKey(columnName);
    }

    @Override
    public GenericRecord record(Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Schema.Field field : schema.getFields()) {
            // note: calling getFieldSchema twice since the field has UNION type,
            // and we only want to extract the non-null field schema in the UNION type.
            // they currently have the same name
            Schema unionSchema = getFieldSchema(field.name(), schema);
            Schema cellSchema = getFieldSchema(field.name(), unionSchema);
            CellData cellData = cellMap.get(field.name());

            // only add the cell if it is not null
            if (cellData != null) {
                builder.set(field.name(), cellData.record(cellSchema));
            }
        }
        return builder.build();
    }

    public RowData copy() {
        RowData copy = new RowData();
        for (CellData cellData : cellMap.values()) {
            copy.addCell(cellData);
        }
        return copy;
    }

    /**
     * Assemble the Avro {@link Schema} for the "after" field of the change event
     * based on the Cassandra table schema.
     * @param tm metadata of a table that contains the Cassandra table schema
     * @return a schema for the "after" field of a change event
     */
    static Schema rowSchema(TableMetadata tm) {
        SchemaBuilder.FieldAssembler assembler = SchemaBuilder.builder().record(AFTER).fields();
        for (ColumnMetadata cm : tm.getColumns()) {
            Schema nullableCellSchema = nullableCellSchema(cm);
            if (nullableCellSchema != null) {
                assembler.name(cm.getName()).type(nullableCellSchema).withDefault(null);
            }
        }
        return (Schema) assembler.endRecord();
    }

    private static Schema nullableCellSchema(ColumnMetadata cm) {
        Schema cellSchema = CellData.cellSchema(cm);
        if (cellSchema != null) {
            return SchemaBuilder.builder().unionOf().nullType().and().type(cellSchema).endUnion();
        } else {
            return null;
        }
    }

    List<CellData> getPrimary() {
        return this.cellMap.values().stream().filter(CellData::isPrimary).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return this.cellMap.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowData rowData = (RowData) o;
        return Objects.equals(cellMap, rowData.cellMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cellMap);
    }
}
