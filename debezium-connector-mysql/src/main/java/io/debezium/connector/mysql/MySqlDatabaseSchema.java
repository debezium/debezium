/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDatabaseSchema;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Component that records the schema history for databases hosted by a MySQL database server. The schema information includes
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link MySqlConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class MySqlDatabaseSchema extends BinlogDatabaseSchema<MySqlPartition, MySqlOffsetContext, MySqlValueConverters, MySqlDefaultValueConverter> {

    /**
     * Create a schema component given the supplied {@link MySqlConnectorConfig MySQL connector configuration}.
     * The DDL statements passed to the schema are parsed and a logical model of the database schema is created.
     *
     */
    public MySqlDatabaseSchema(MySqlConnectorConfig connectorConfig, MySqlValueConverters valueConverter, TopicNamingStrategy<TableId> topicNamingStrategy,
                               SchemaNameAdjuster schemaNameAdjuster, boolean tableIdCaseInsensitive) {
        super(connectorConfig,
                valueConverter,
                new MySqlDefaultValueConverter(valueConverter),
                topicNamingStrategy,
                schemaNameAdjuster,
                tableIdCaseInsensitive);
    }

    @Override
    protected DdlParser createDdlParser(BinlogConnectorConfig connectorConfig, MySqlValueConverters valueConverter) {
        return new MySqlAntlrDdlParser(
                true,
                false,
                connectorConfig.isSchemaCommentsHistoryEnabled(),
                valueConverter,
                getTableFilter(),
                connectorConfig.getCharsetRegistry());
    }

}
