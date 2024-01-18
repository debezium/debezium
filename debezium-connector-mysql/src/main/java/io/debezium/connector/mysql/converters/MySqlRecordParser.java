/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * Parser for records produced by the MySQL connector.
 *
 * @author Chris Cranford
 */
public class MySqlRecordParser extends RecordParser {

    static final String TABLE_NAME_KEY = "table";
    static final String SERVER_ID_KEY = "server_id";
    static final String GTID_KEY = "gtid";
    static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
    static final String THREAD_KEY = "thread";
    static final String QUERY_KEY = "query";

    static final Set<String> MYSQL_SOURCE_FIELDS = Collect.unmodifiableSet(
            TABLE_NAME_KEY,
            SERVER_ID_KEY,
            GTID_KEY,
            BINLOG_FILENAME_OFFSET_KEY,
            BINLOG_POSITION_OFFSET_KEY,
            BINLOG_ROW_IN_EVENT_OFFSET_KEY,
            THREAD_KEY,
            QUERY_KEY);

    public MySqlRecordParser(RecordAndMetadata recordAndMetadata) {
        super(recordAndMetadata, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (MYSQL_SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from MySQL connector");
    }
}
