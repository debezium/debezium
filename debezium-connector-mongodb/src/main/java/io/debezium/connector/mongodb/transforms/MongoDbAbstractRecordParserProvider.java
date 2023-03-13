/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.converters.MongoDbRecordParser;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.RecordParserProvider;

public abstract class MongoDbAbstractRecordParserProvider implements RecordParserProvider {

    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        return new MongoDbRecordParser(schema, record);
    }
}
