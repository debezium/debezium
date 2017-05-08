package io.debezium.connector.mongodb;

import com.datapipeline.base.mongodb.ColumnData;
import com.datapipeline.base.mongodb.ColumnSchema;

public class MongoDbColumnData implements ColumnData {

    private final Object value;

    private final ColumnSchema columnSchema;

    public MongoDbColumnData(Object value, ColumnSchema columnSchema) {
        this.value = value;
        this.columnSchema = columnSchema;
    }

    @Override
    public ColumnSchema getColumn() {
        return columnSchema;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
