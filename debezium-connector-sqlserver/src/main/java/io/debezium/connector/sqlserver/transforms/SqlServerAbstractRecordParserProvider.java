/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.converters.SqlServerRecordParser;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.RecordParserProvider;

public abstract class SqlServerAbstractRecordParserProvider implements RecordParserProvider {

    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        return new SqlServerRecordParser(schema, record);
    }
}
