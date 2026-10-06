/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Builds the Kafka Connect schema and struct for the {@code source} field
 * embedded in every change event envelope.
 *
 * <p>Add connector-specific fields to {@link #init} and populate them in {@link #struct}.
 * Any field added here must also be stored in {@link ${connectorName}SourceInfo}.
 */
class ${connectorName}SourceInfoStructMaker extends AbstractSourceInfoStructMaker<${connectorName}SourceInfo> {

    private static final String POSITION_KEY = "position";

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig config) {
        super.init(connector, version, config);
        schema = commonSchemaBuilder()
                .name("${package}.Source")
                .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(POSITION_KEY, Schema.INT64_SCHEMA)
                // Add further connector-specific source fields here.
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(${connectorName}SourceInfo info) {
        final Struct result = commonStruct(info);
        result.put(AbstractSourceInfo.SCHEMA_NAME_KEY, info.schemaName());
        result.put(AbstractSourceInfo.TABLE_NAME_KEY, info.tableName());
        result.put(POSITION_KEY, info.getPosition());
        // Populate further connector-specific fields here.
        return result;
    }
}
