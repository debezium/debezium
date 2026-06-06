/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Logical representation of SQL Server schema.
 *
 * @author Jiri Pechanec
 */
public class SqlServerDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDatabaseSchema.class);

    public SqlServerDatabaseSchema(SqlServerConnectorConfig connectorConfig, SqlServerDefaultValueConverter defaultValueConverter,
                                   ValueConverterProvider valueConverter, TopicNamingStrategy<TableId> topicNamingStrategy,
                                   SchemaNameAdjuster schemaNameAdjuster, CustomConverterRegistry customConverterRegistry,
                                   CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverter,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        customConverterRegistry,
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        true,
                        connectorConfig.getEventConvertingFailureHandlingMode()),
                false, connectorConfig.getKeyMapper(), taskContext);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        // just a single table per DDL event for SQL Server
        Table table = schemaChange.getTables().iterator().next();
        buildAndRegisterSchema(table);
        tables().overwriteTable(table);

        TableChanges tableChanges = null;
        if (schemaChange.getType() == SchemaChangeEventType.CREATE) {
            tableChanges = new TableChanges();
            tableChanges.create(table);
        }
        else if (schemaChange.getType() == SchemaChangeEventType.ALTER) {
            tableChanges = new TableChanges();
            tableChanges.alter(table);
        }

        record(schemaChange, tableChanges);
    }

    @Override
    protected DdlParser getDdlParser() {
        return null;
    }

    /**
     * Returns whether the provided column is a max-type column ({@code varchar(max)},
     * {@code nvarchar(max)}, or {@code varbinary(max)}) whose NULL value in the CDC
     * capture table should be replaced by the {@code unavailable.value.placeholder}
     * when the column was not changed during an UPDATE.
     *
     * @param column the relational column model
     * @return {@code true} if the column is a max-type column
     */
    public static boolean isMaxColumn(Column column) {
        return isMaxColumnJdbcType(column.jdbcType());
    }

    /**
     * Returns whether the provided JDBC type represents a max-type column
     * ({@code varchar(max)}, {@code nvarchar(max)}, or {@code varbinary(max)}).
     *
     * @param jdbcType the JDBC type code
     * @return {@code true} if the JDBC type is a max-type
     */
    public static boolean isMaxColumnJdbcType(int jdbcType) {
        return jdbcType == Types.LONGVARCHAR
                || jdbcType == Types.LONGNVARCHAR
                || jdbcType == Types.LONGVARBINARY;
    }

}
