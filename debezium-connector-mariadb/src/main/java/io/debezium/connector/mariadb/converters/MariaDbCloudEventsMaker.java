/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.util.Collect;

/**
 * A {@link CloudEventsMaker} implementation for records produced by the MariaDB connector.
 *
 * @author Chris Cranford
 */
public class MariaDbCloudEventsMaker extends CloudEventsMaker {

    static final String TABLE_NAME_KEY = "table";
    static final String SERVER_ID_KEY = "server_id";
    static final String GTID_KEY = "gtid";
    static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
    static final String THREAD_KEY = "thread";
    static final String QUERY_KEY = "query";

    static final Set<String> MARIADB_SOURCE_FIELDS = Collect.unmodifiableSet(
            TABLE_NAME_KEY,
            SERVER_ID_KEY,
            GTID_KEY,
            BINLOG_FILENAME_OFFSET_KEY,
            BINLOG_POSITION_OFFSET_KEY,
            BINLOG_ROW_IN_EVENT_OFFSET_KEY,
            THREAD_KEY,
            QUERY_KEY);

    public MariaDbCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase, String schemaName) {
        super(recordAndMetadata, contentType, dataSchemaUriBase, schemaName);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";file:" + sourceField(BINLOG_FILENAME_OFFSET_KEY)
                + ";pos:" + sourceField(BINLOG_POSITION_OFFSET_KEY);
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return MARIADB_SOURCE_FIELDS;
    }
}
