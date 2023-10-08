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

import io.debezium.converters.CloudEventsConverterConfig;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;

public class RecordAndMetadataHeaderImpl extends RecordAndMetadataBaseImpl implements RecordAndMetadata {

    private final String id;
    private final String type;
    private final Struct source;
    private final String operation;
    private final Struct transaction;
    private final SchemaAndValue ts_ms;

    public RecordAndMetadataHeaderImpl(Struct record, Schema dataSchema, Headers headers, JsonConverter jsonHeaderConverter,
                                       CloudEventFieldsSources cloudEventFieldsSources) {
        super(record, dataSchema);

        if (cloudEventFieldsSources.getIdSource() == CloudEventsConverterConfig.IdSource.HEADER) {
            this.id = (String) getHeaderSchemaAndValue(headers, CloudEventsMaker.FieldName.ID, jsonHeaderConverter).value();
        }
        else {
            this.id = super.id();
        }

        if (cloudEventFieldsSources.getTypeSource() == CloudEventsConverterConfig.TypeSource.HEADER) {
            this.type = (String) getHeaderSchemaAndValue(headers, CloudEventsMaker.FieldName.TYPE, jsonHeaderConverter).value();
        }
        else {
            this.type = super.type();
        }

        if (cloudEventFieldsSources.getMetadataSource() == CloudEventsConverterConfig.MetadataLocation.HEADER) {
            this.source = (Struct) getHeaderSchemaAndValue(headers, Envelope.FieldName.SOURCE, jsonHeaderConverter).value();
            this.operation = (String) getHeaderSchemaAndValue(headers, Envelope.FieldName.OPERATION, jsonHeaderConverter).value();
            this.transaction = (Struct) getHeaderSchemaAndValue(headers, Envelope.FieldName.TRANSACTION, jsonHeaderConverter).value();
        }
        else {
            this.source = super.source();
            this.operation = super.operation();
            this.transaction = super.transaction();
        }

        String ts_ms = source.getInt64(Envelope.FieldName.TIMESTAMP).toString();
        Schema ts_msSchema = source.schema().field(Envelope.FieldName.TIMESTAMP).schema();
        this.ts_ms = new SchemaAndValue(ts_msSchema, ts_ms);
    }

    @Override
    public Schema dataSchema(String... dataFields) {
        return super.dataSchema;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public Struct source() {
        return this.source;
    }

    @Override
    public String operation() {
        return this.operation;
    }

    @Override
    public Struct transaction() {
        return this.transaction;
    }

    @Override
    public SchemaAndValue timestamp() {
        return this.ts_ms;
    }

    private static SchemaAndValue getHeaderSchemaAndValue(Headers headers, String headerName, JsonConverter jsonHeaderConverter) {
        Header header = headers.lastHeader(headerName);
        if (header == null) {
            throw new RuntimeException("Header `" + headerName + "` was not provided");
        }
        return jsonHeaderConverter.toConnectData(null, header.value());
    }
}
