/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker for records producer by the PostgreSQL connector.
 *
 * @author Chris Cranford
 */
public class PostgresCloudEventsMaker extends CloudEventsMaker {

    static final String TXID_KEY = "txId";
    static final String XMIN_KEY = "xmin";
    static final String LSN_KEY = "lsn";
    static final String SEQUENCE_KEY = "sequence";

    static final Set<String> POSTGRES_SOURCE_FIELDS = Collect.unmodifiableSet(
            TXID_KEY,
            XMIN_KEY,
            LSN_KEY,
            SEQUENCE_KEY);

    public PostgresCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                    String cloudEventsSchemaName) {
        super(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + sourceField(LSN_KEY).toString()
                + ";txId:" + sourceField(TXID_KEY).toString()
                + ";sequence:" + sourceField(SEQUENCE_KEY).toString();
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return POSTGRES_SOURCE_FIELDS;
    }
}
