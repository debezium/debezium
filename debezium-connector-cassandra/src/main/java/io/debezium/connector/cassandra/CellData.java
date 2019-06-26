/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

import java.util.Objects;

/**
 * Cell-level data about the source event. Each cell contains the name, value and
 * type of a column in a Cassandra table.
 */
public class CellData implements AvroRecord {
    /**
     * The type of a column in a Cassandra table
     */
    public enum ColumnType {
        /**
         * A partition column is responsible for data distribution across nodes for this table.
         * Every Cassandra table must have at least one partition column.
         */
        PARTITION,

        /**
         * A clustering column is used to specifies the order that the data is arranged inside the partition.
         * A Cassandra table may not have any clustering column,
         */
        CLUSTERING,

        /**
         * A regular column is a column that is not a partition or a clustering column.
         */
        REGULAR
    }

    public static final String CELL_VALUE_KEY = "value";
    public static final String CELL_DELETION_TS_KEY = "deletion_ts";
    public static final String CELL_SET_KEY = "set";

    public final String name;
    public final Object value;
    public final Object deletionTs;
    public final ColumnType columnType;

    public CellData(String name, Object value, Object deletionTs, ColumnType columnType) {
        this.name = name;
        this.value = value;
        this.deletionTs = deletionTs;
        this.columnType = columnType;
    }

    public boolean isPrimary() {
        return columnType == ColumnType.PARTITION || columnType == ColumnType.CLUSTERING;
    }

    @Override
    public GenericRecord record(Schema schema) {
        return new GenericRecordBuilder(schema)
                .set(CELL_VALUE_KEY, value)
                .set(CELL_DELETION_TS_KEY, deletionTs)
                .set(CELL_SET_KEY, true)
                .build();
    }

    static Schema cellSchema(ColumnMetadata cm) {
        AbstractType<?> convertedType = CassandraTypeConverter.convert(cm.getType());
        Schema valueSchema = CassandraTypeToAvroSchemaMapper.getSchema(convertedType, true);
        if (valueSchema != null) {
            return SchemaBuilder.builder().record(cm.getName()).fields()
                    .name(CELL_VALUE_KEY).type(valueSchema).noDefault()
                    .name(CELL_DELETION_TS_KEY).type(CassandraTypeToAvroSchemaMapper.nullable(CassandraTypeToAvroSchemaMapper.LONG_TYPE)).withDefault(null)
                    .name(CELL_SET_KEY).type().booleanType().noDefault()
                    .endRecord();
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CellData that = (CellData) o;
        return Objects.equals(name, that.name)
                && Objects.equals(value, that.value)
                && deletionTs == that.deletionTs
                && columnType == that.columnType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, deletionTs, columnType);
    }

    @Override
    public String toString() {
        return "{"
                + "name=" + name
                + ", value=" + value
                + ", deletionTs=" + deletionTs
                + ", type=" + columnType.name()
                + '}';
    }
}
