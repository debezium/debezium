package io.debezium.connector.mongodb;

import com.datapipeline.base.converter.ColumnData;
import com.datapipeline.base.converter.ColumnSchema;

/**
 * Created by hanlinliu on 12/14/16.
 */
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
