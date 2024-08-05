/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters.recordandmetadata;

import java.util.function.Supplier;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.converters.CloudEventsConverterConfig.MetadataSource;
import io.debezium.converters.CloudEventsConverterConfig.MetadataSourceValue;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;

public class RecordAndMetadataHeaderImpl extends RecordAndMetadataBaseImpl implements RecordAndMetadata {

    private final Headers headers;
    private final MetadataSource metadataSource;
    private final JsonConverter jsonHeaderConverter;

    public RecordAndMetadataHeaderImpl(Struct record, Schema originalDataSchema, Headers headers, MetadataSource metadataSource, JsonConverter jsonHeaderConverter) {
        super(record, originalDataSchema);
        this.headers = headers;
        this.metadataSource = metadataSource;
        this.jsonHeaderConverter = jsonHeaderConverter;
    }

    @Override
    public String id() {
        return getValueFromHeaderOrByDefault(metadataSource.id(), CloudEventsMaker.FieldName.ID, false, null, super::id);
    }

    @Override
    public String type() {
        return getValueFromHeaderOrByDefault(metadataSource.type(), CloudEventsMaker.FieldName.TYPE, false, null, super::type);
    }

    @Override
    public Struct source() {
        return getValueFromHeaderOrByDefault(metadataSource.global(), Envelope.FieldName.SOURCE, false, null, super::source);
    }

    @Override
    public String operation() {
        return getValueFromHeaderOrByDefault(metadataSource.global(), Envelope.FieldName.OPERATION, false, null, super::operation);
    }

    @Override
    public Struct transaction() {
        return getValueFromHeaderOrByDefault(metadataSource.global(), Envelope.FieldName.TRANSACTION, true, null, super::transaction);
    }

    @Override
    public SchemaAndValue timestamp() {
        return getValueFromHeaderOrByDefault(metadataSource.global(), null, null, () -> {
            String ts_ms = this.source().getInt64(Envelope.FieldName.TIMESTAMP).toString();
            Schema ts_msSchema = this.source().schema().field(Envelope.FieldName.TIMESTAMP).schema();
            return new SchemaAndValue(ts_msSchema, ts_ms);
        }, super::timestamp);
    }

    @Override
    public String dataSchemaName() {
        return getValueFromHeaderOrByDefault(metadataSource.dataSchemaName(), CloudEventsMaker.DATA_SCHEMA_NAME_PARAM, false, null, super::dataSchemaName);
    }

    @Override
    public Schema dataSchema(String... dataFields) {
        return getValueFromHeaderOrByDefault(metadataSource.global(), null, null, super::dataSchema, () -> super.dataSchema(dataFields));
    }

    private <T> T getValueFromHeaderOrByDefault(MetadataSourceValue metadataSourceValue,
                                                String headerName,
                                                Boolean headerIsOptional,
                                                Supplier<T> headerCaseDefaultSupplier,
                                                Supplier<T> defaultSupplier) {
        if (metadataSourceValue == MetadataSourceValue.HEADER) {
            if (headerName != null) {
                return (T) (getHeaderSchemaAndValue(headers, headerName, headerIsOptional).value());
            }
            return headerCaseDefaultSupplier.get();
        }
        return defaultSupplier.get();
    }

    private SchemaAndValue getHeaderSchemaAndValue(Headers headers, String headerName, boolean isOptional) {
        Header header = headers.lastHeader(headerName);
        if (header == null) {
            if (isOptional) {
                return SchemaAndValue.NULL;
            }
            else {
                throw new RuntimeException("Header `" + headerName + "` was not provided");
            }
        }
        return jsonHeaderConverter.toConnectData(null, header.value());
    }
}
