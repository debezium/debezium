/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.converters;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * Parser for records producer by the MongoDB connector.
 *
 * @author Chris Cranford
 */
public class MongoDbRecordParser extends RecordParser {

    static final String REPLICA_SET_NAME = "rs";
    static final String ORDER = "ord";
    static final String OPERATION_ID = "h";
    static final String COLLECTION = "collection";

    static final Set<String> MONGODB_SOURCE_FIELD = Collect.unmodifiableSet(
            REPLICA_SET_NAME,
            ORDER,
            OPERATION_ID,
            COLLECTION);

    public MongoDbRecordParser(Schema schema, Struct record) {
        super(schema, record, Envelope.FieldName.AFTER, "patch", "filter");
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (MONGODB_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from MongoDB connector");
    }
}
