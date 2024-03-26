/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.converters;

import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * Parser for records produced by PostgreSQL connectors.
 *
 * @author Chris Cranford
 */
public class PostgresRecordParser extends RecordParser {

    static final String TABLE_NAME_KEY = "table";
    static final String TXID_KEY = "txId";
    static final String XMIN_KEY = "xmin";
    static final String LSN_KEY = "lsn";
    static final String SEQUENCE_KEY = "sequence";

    static final Set<String> POSTGRES_SOURCE_FIELD = Collect.unmodifiableSet(
            TABLE_NAME_KEY,
            TXID_KEY,
            XMIN_KEY,
            LSN_KEY,
            SEQUENCE_KEY);

    public PostgresRecordParser(RecordAndMetadata recordAndMetadata) {
        super(recordAndMetadata, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (POSTGRES_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from PostgreSQL connector");
    }
}
