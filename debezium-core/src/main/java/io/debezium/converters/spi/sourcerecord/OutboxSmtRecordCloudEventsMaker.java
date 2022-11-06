/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi.sourcerecord;

import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for source records
 *
 * @author Roman Kudryashov
 */
public class OutboxSmtRecordCloudEventsMaker extends CloudEventsMaker {

    public OutboxSmtRecordCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return this.recordParser.getMetadata(FieldName.ID).toString();
    }

    @Override
    public String ceType() {
        return ceDataAttribute().get(FieldName.TYPE).toString();
    }
}
