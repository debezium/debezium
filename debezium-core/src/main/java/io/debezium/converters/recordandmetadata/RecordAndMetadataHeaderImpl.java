/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.recordandmetadata;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.converters.CloudEventsConverterConfig.MetadataLocation;
import io.debezium.converters.CloudEventsConverterConfig.MetadataLocationValue;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;

public class RecordAndMetadataHeaderImpl extends RecordAndMetadataBaseImpl implements RecordAndMetadata {

    private final Headers headers;
    private final MetadataLocation metadataLocation;
    private final JsonConverter jsonHeaderConverter;

    public RecordAndMetadataHeaderImpl(Struct record, Schema dataSchema, Headers headers, MetadataLocation metadataLocation, JsonConverter jsonHeaderConverter) {
        super(record, dataSchema);
        this.headers = headers;
        this.metadataLocation = metadataLocation;
        this.jsonHeaderConverter = jsonHeaderConverter;
    }

    @Override
    public Schema dataSchema(String... dataFields) {
        return super.dataSchema;
    }

    @Override
    public String id() {
        if (metadataLocation.id() == MetadataLocationValue.HEADER) {
            return (String) getHeaderSchemaAndValue(headers, CloudEventsMaker.FieldName.ID, false, jsonHeaderConverter).value();
        }
        return super.id();
    }

    @Override
    public String type() {
        if (metadataLocation.type() == MetadataLocationValue.HEADER) {
            return (String) getHeaderSchemaAndValue(headers, CloudEventsMaker.FieldName.TYPE, false, jsonHeaderConverter).value();
        }
        return super.type();
    }

    @Override
    public Struct source() {
        if (metadataLocation.global() == MetadataLocationValue.HEADER) {
            return (Struct) getHeaderSchemaAndValue(headers, Envelope.FieldName.SOURCE, false, jsonHeaderConverter).value();
        }
        return super.source();
    }

    @Override
    public String operation() {
        if (metadataLocation.global() == MetadataLocationValue.HEADER) {
            return (String) getHeaderSchemaAndValue(headers, Envelope.FieldName.OPERATION, false, jsonHeaderConverter).value();
        }
        return super.operation();
    }

    @Override
    public Struct transaction() {
        if (metadataLocation.global() == MetadataLocationValue.HEADER) {
            return (Struct) getHeaderSchemaAndValue(headers, Envelope.FieldName.TRANSACTION, true, jsonHeaderConverter).value();
        }
        return super.transaction();
    }

    @Override
    public SchemaAndValue timestamp() {
        if (metadataLocation.global() == MetadataLocationValue.HEADER) {
            String ts_ms = this.source().getInt64(Envelope.FieldName.TIMESTAMP).toString();
            Schema ts_msSchema = this.source().schema().field(Envelope.FieldName.TIMESTAMP).schema();
            return new SchemaAndValue(ts_msSchema, ts_ms);
        }

        return super.timestamp();
    }

    private static SchemaAndValue getHeaderSchemaAndValue(Headers headers, String headerName, boolean isOptional, JsonConverter jsonHeaderConverter) {
        Header header = headers.lastHeader(headerName);
        if (header == null && !isOptional) {
            throw new RuntimeException("Header `" + headerName + "` was not provided");
        }
        return jsonHeaderConverter.toConnectData(null, header.value());
    }
}
