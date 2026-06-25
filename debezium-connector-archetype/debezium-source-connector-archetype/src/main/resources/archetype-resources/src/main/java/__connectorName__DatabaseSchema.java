/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.sql.SQLException;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Holds the relational schema (tables, columns, and their Kafka Connect schemas) for the
 * ${connectorName} connector.
 *
 * <p>Extends {@link RelationalDatabaseSchema}, the non-historized base: the schema is read
 * live from the database on startup and is not persisted to a schema history topic. Call
 * {@link #refresh} to (re)load it from the database.
 *
 * <p>The schema uses {@link JdbcValueConverters} to map standard JDBC types to Kafka Connect
 * types. Replace it with a connector-specific {@code ValueConverterProvider} if your database
 * has types the default converter does not handle.
 */
public class ${connectorName}DatabaseSchema extends RelationalDatabaseSchema {

    public ${connectorName}DatabaseSchema(${connectorName}ConnectorConfig config,
                                          TopicNamingStrategy<TableId> topicNamingStrategy,
                                          CdcSourceTaskContext<${connectorName}ConnectorConfig> taskContext) {
        super(config, topicNamingStrategy,
                config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        new JdbcValueConverters(),
                        null,
                        config.schemaNameAdjuster(),
                        config.getServiceRegistry().tryGetService(CustomConverterRegistry.class),
                        config.getSourceInfoStructMaker().schema(),
                        config.getFieldNamer(),
                        false,
                        config.getEventConvertingFailureHandlingMode()),
                false,
                config.getKeyMapper(),
                taskContext);
    }

    /**
     * Reads the structure of the captured tables from the database and rebuilds their Kafka
     * Connect schemas. Call this on startup and whenever the source signals a DDL change.
     */
    public void refresh(JdbcConnection connection) throws SQLException {
        connection.readSchema(tables(), null, null, getTableFilter(), null, true);
        clearSchemas();
        tableIds().forEach(this::refreshSchema);
    }
}
