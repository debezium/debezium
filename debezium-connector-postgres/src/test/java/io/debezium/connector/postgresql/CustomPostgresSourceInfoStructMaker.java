/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;

public class CustomPostgresSourceInfoStructMaker extends PostgresSourceInfoStructMaker {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.postgresql.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.XMIN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field("newField", Schema.STRING_SCHEMA) // new field
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct result = super.struct(sourceInfo);
        result.put("newField", "newFieldValue");
        return result;
    }
}
