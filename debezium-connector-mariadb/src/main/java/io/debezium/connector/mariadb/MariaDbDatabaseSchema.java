/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDatabaseSchema;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.relational.TableId;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * A concrete implementation of database schema for MariaDB databases.
 *
 * @author Chris Cranford
 */
public class MariaDbDatabaseSchema extends BinlogDatabaseSchema<MariaDbPartition, MariaDbOffsetContext, MariaDbValueConverters, MariaDbDefaultValueConverter> {

    public MariaDbDatabaseSchema(MariaDbConnectorConfig connectorConfig, MariaDbValueConverters valueConverter,
                                 TopicNamingStrategy<TableId> topicNamingStrategy, SchemaNameAdjuster schemaNameAdjuster,
                                 boolean tableIdCaseInsensitive) {
        super(connectorConfig,
                valueConverter,
                new MariaDbDefaultValueConverter(valueConverter),
                topicNamingStrategy,
                schemaNameAdjuster,
                tableIdCaseInsensitive);
    }

    @Override
    protected DdlParser createDdlParser(BinlogConnectorConfig connectorConfig, MariaDbValueConverters valueConverter) {
        return new MariaDbAntlrDdlParser(
                true,
                false,
                connectorConfig.isSchemaChangesHistoryEnabled(),
                valueConverter,
                getTableFilter(),
                connectorConfig.getCharsetRegistry());
    }

}
