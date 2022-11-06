/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi.sourcerecord;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.converters.spi.RecordParser;

/**
 * Parser for source records
 *
 * @author Roman Kudryashov
 */
public class OutboxSmtRecordParser extends RecordParser {

    private final Map<String, Object> recordHeaders = new HashMap<>();

    public OutboxSmtRecordParser(Schema schema, SourceRecord record) {
        super(schema, record);
        for (Header header : record.headers()) {
            recordHeaders.put(header.key(), header.value());
        }
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (recordHeaders.containsKey(name)) {
            return recordHeaders.get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field");
    }
}
