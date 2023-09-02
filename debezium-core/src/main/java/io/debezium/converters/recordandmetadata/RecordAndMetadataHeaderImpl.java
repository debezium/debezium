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

import io.debezium.data.Envelope;

public class RecordAndMetadataHeaderImpl implements RecordAndMetadata {

    private final Struct record;
    private final Schema dataSchema;
    private final Struct source;
    private final String operation;
    private final Struct transaction;
    private final SchemaAndValue ts_ms;

    public RecordAndMetadataHeaderImpl(Struct record, Schema dataSchema, Headers headers, JsonConverter jsonHeaderConverter) {
        this.record = record;
        this.dataSchema = dataSchema;
        this.source = (Struct) getHeaderSchemaAndValue(headers, Envelope.FieldName.SOURCE, jsonHeaderConverter).value();
        this.operation = (String) getHeaderSchemaAndValue(headers, Envelope.FieldName.OPERATION, jsonHeaderConverter).value();
        this.transaction = (Struct) getHeaderSchemaAndValue(headers, Envelope.FieldName.TRANSACTION, jsonHeaderConverter).value();
        String ts_ms = source.getInt64(Envelope.FieldName.TIMESTAMP).toString();
        Schema ts_msSchema = source.schema().field(Envelope.FieldName.TIMESTAMP).schema();
        this.ts_ms = new SchemaAndValue(ts_msSchema, ts_ms);
    }

    @Override
    public Struct record() {
        return this.record;
    }

    @Override
    public Schema dataSchema(String... dataFields) {
        return this.dataSchema;
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
    public SchemaAndValue ts_ms() {
        return this.ts_ms;
    }

    private static SchemaAndValue getHeaderSchemaAndValue(Headers headers, String headerName, JsonConverter jsonHeaderConverter) {
        Header header = headers.lastHeader(headerName);
        return jsonHeaderConverter.toConnectData(null, header.value());
    }
}
