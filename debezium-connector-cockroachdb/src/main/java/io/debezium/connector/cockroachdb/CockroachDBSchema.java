/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0,
 * available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.Set;

import io.debezium.connector.cockroachdb.connection.CockroachDBConnection;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalTables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Provides access to the relational database schema information for CockroachDB.
 * Used to convert changefeed messages into schema-aware Kafka Connect records.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSchema extends RelationalDatabaseSchema {

    private final CockroachDBConnection connection;

    public CockroachDBSchema(
                             CockroachDBConnectorConfig config,
                             CockroachDBConnection connection,
                             TopicNamingStrategy<TableId> topicNamingStrategy,
                             SchemaNameAdjuster nameAdjuster) {

        super(
                config,
                new CockroachDBValueConverter(config),
                connection,
                TableSchemaBuilder.create()
                        .valueConverter(new CockroachDBValueConverter(config))
                        .schemaNameAdjuster(nameAdjuster)
                        .unknownDataTypeResolver(new CockroachDBUnknownTypeResolver(config)),
                topicNamingStrategy,
                nameAdjuster);

        this.connection = connection;
    }

    public CockroachDBConnection getConnection() {
        return connection;
    }

    public Set<Table> tableSchemas() {
        RelationalTables tables = schema().tables();
        return tables.asTableSet();
    }
}
