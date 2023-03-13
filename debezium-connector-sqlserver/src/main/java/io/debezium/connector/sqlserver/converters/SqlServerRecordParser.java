/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.converters;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * Parser for records produced by the SQL Server connector.
 *
 * @author Chris Cranford
 */
public class SqlServerRecordParser extends RecordParser {

    static final String CHANGE_LSN_KEY = "change_lsn";
    static final String COMMIT_LSN_KEY = "commit_lsn";
    static final String EVENT_SERIAL_NO_KEY = "event_serial_no";
    static final Set<String> SQLSERVER_SOURCE_FIELD = Collect.unmodifiableSet(
            CHANGE_LSN_KEY,
            COMMIT_LSN_KEY,
            EVENT_SERIAL_NO_KEY,
            AbstractSourceInfo.TABLE_NAME_KEY,
            AbstractSourceInfo.SCHEMA_NAME_KEY);

    public SqlServerRecordParser(Schema schema, Struct record) {
        super(schema, record, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (SQLSERVER_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from SQLServer connector");
    }
}
