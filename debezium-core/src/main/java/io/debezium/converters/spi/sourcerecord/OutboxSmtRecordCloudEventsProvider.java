/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi.sourcerecord;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for source records
 *
 * @author Roman Kudryashov
 */
public class OutboxSmtRecordCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return "source_record";
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        throw new DataException("Parser not implemented");
    }

    @Override
    public RecordParser createParser(Schema schema, SourceRecord record) {
        return new OutboxSmtRecordParser(schema, record);
    }

    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new OutboxSmtRecordCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}
