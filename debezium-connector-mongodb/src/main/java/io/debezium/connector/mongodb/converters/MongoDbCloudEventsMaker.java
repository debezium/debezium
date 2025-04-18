/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker for records producer by MongoDB connector.
 *
 * @author Chris Cranford
 */
public class MongoDbCloudEventsMaker extends CloudEventsMaker {

    static final String REPLICA_SET_NAME = "rs";
    static final String ORDER = "ord";
    static final String COLLECTION = "collection";
    static final String WALL_TIME = "wallTime";

    static final Set<String> MONGODB_SOURCE_FIELDS = Collect.unmodifiableSet(
            REPLICA_SET_NAME,
            ORDER,
            COLLECTION,
            WALL_TIME);

    public MongoDbCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                   String cloudEventsSchemaName) {
        super(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER,
                MongoDbFieldName.UPDATE_DESCRIPTION);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";ts:" + sourceField(AbstractSourceInfo.TIMESTAMP_KEY)
                + ";ord:" + sourceField(ORDER);
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return MONGODB_SOURCE_FIELDS;
    }
}
