/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.snapshot.spi.SnapshotQuery;
import oracle.jdbc.OracleTypes;

@ConnectorSpecific(connector = OracleConnector.class)
public class SelectAllSnapshotQuery implements SnapshotQuery, BeanRegistryAware {

    private BeanRegistry beanRegistry;

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext = beanRegistry.lookupByName(StandardBeanNames.SNAPSHOT_CONTEXT,
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext.class);
        final OracleOffsetContext offset = (OracleOffsetContext) snapshotContext.offset;

        final String snapshotOffset = offset.getScn().toString();

        TableId table = TableId.parse(tableId);
        String columns = getColumns(snapshotContext.tables.forTable(new TableId(snapshotContext.catalogName, table.catalog(), table.table())));
        assert snapshotOffset != null;
        return Optional.of(String.format("SELECT %s FROM %s AS OF SCN %s", columns, tableId, snapshotOffset));
    }

    private String getColumns(Table table) {
        StringBuilder columnsSb = new StringBuilder();
        for (Column column : table.columns()) {
            switch (column.jdbcType()) {
                case OracleTypes.TIMESTAMP:
                case OracleTypes.TIMESTAMPLTZ:
                    if (column.typeName().trim().equalsIgnoreCase("DATE")) {
                        columnsSb.append(String.format("TO_CHAR(%s, 'SYYYY-MM-DD HH24:MI:SS') AS %s, ", column.name(), column.name()));
                    }
                    else {
                        columnsSb.append(String.format("TO_CHAR(%s, 'SYYYY-MM-DD HH24:MI:SS.FF') AS %s, ", column.name(), column.name()));
                    }
                    break;
                case OracleTypes.TIMESTAMPTZ:
                    columnsSb.append(String.format("TO_CHAR(%s, 'SYYYY-MM-DD HH24:MI:SS.FFTZH:TZM') AS %s, ", column.name(), column.name()));
                    break;
                default:
                    columnsSb.append(String.format("%s, ", column.name()));
            }
        }
        return columnsSb.substring(0, columnsSb.length() - 2);
    }
}
