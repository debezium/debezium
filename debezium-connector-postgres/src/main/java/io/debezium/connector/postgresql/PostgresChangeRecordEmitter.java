/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.jdbc.PgConnection;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * Emits change data based on a logical decoding event coming as protobuf or JSON message.
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final ReplicationMessage message;
    private final PostgresSchema schema;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection connection;

    public PostgresChangeRecordEmitter(OffsetContext offset, Clock clock, PostgresConnectorConfig connectorConfig, PostgresSchema schema, PostgresConnection connection, ReplicationMessage message) {
        super(offset, clock);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.connection = connection;
    }

    @Override
    protected Operation getOperation() {
        switch (message.getOperation()) {
        case INSERT:
            return Operation.CREATE;
        case UPDATE:
            return Operation.UPDATE;
        case DELETE:
            return Operation.DELETE;
        default:
            throw new IllegalArgumentException("Received event of unexpected command type: " + message.getOperation());
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        final TableId tableId = PostgresSchema.parse(message.getTable());
        Objects.requireNonNull(tableId);

        try {
            switch (getOperation()) {
                case CREATE:
                    return null;
                case UPDATE:
                    return columnValues(message.getOldTupleList(), tableId, true, message.hasTypeMetadata());
                default:
                    return columnValues(message.getOldTupleList(), tableId, true, message.hasTypeMetadata());
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        final TableId tableId = PostgresSchema.parse(message.getTable());
        Objects.requireNonNull(tableId);

        try {
            switch (getOperation()) {
                case CREATE:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata());
                case UPDATE:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata());
                default:
                    return null;
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public DataCollectionSchema synchronizeTableSchema(DataCollectionSchema tableSchema) {
        final boolean metadataInMessage = message.hasTypeMetadata();
        final TableId tableId = (TableId) tableSchema.id();
        final Table table = schema.tableFor(tableId);
        final List<ReplicationMessage.Column> columns = getOperation() == Operation.DELETE ? message.getOldTupleList() : message.getNewTupleList();
        // check if we need to refresh our local schema due to DB schema changes for this table
        if (schemaChanged(columns, table, metadataInMessage)) {
            // Refresh the schema so we get information about primary keys
            refreshTableFromDatabase(tableId);
            // Update the schema with metadata coming from decoder message
            if (metadataInMessage) {
                schema.refresh(tableFromFromMessage(columns, schema.tableFor(tableId)));
            }
        }
        return schema.schemaFor(tableId);
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns, TableId tableId, boolean refreshSchemaIfChanged, boolean metadataInMessage)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        final Table table = schema.tableFor(tableId);
        Objects.requireNonNull(table);

        // based on the schema columns, create the values on the same position as the columns
        List<Column> schemaColumns = table.columns();
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columns.size() < schemaColumns.size() ? schemaColumns.size() : columns.size()];

        for (ReplicationMessage.Column column: columns) {
            //DBZ-298 Quoted column names will be sent like that in messages, but stored unquoted in the column names
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            final Column tableColumn = table.columnWithName(columnName);
            if (tableColumn == null) {
                logger.warn(
                        "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                        column.getName());
                continue;
            }
            int position = tableColumn.position() - 1;
            if (position < 0 || position >= values.length) {
                logger.warn(
                        "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                        column.getName());
                continue;
            }
            values[position] = column.getValue(() -> (PgConnection) connection.connection(), connectorConfig.includeUnknownDatatypes());
        }

        return values;
    }

    private Optional<DataCollectionSchema> newTable(TableId tableId) {
        logger.debug("Schema for table '{}' is missing", tableId);
        refreshTableFromDatabase(tableId);
        final TableSchema tableSchema = schema.schemaFor(tableId);
        if (tableSchema == null) {
            logger.warn("cannot load schema for table '{}'", tableId);
            return Optional.empty();
        }
        else {
            logger.debug("refreshed DB schema to include table '{}'", tableId);
            return Optional.of(tableSchema);
        }
    }

    private void refreshTableFromDatabase(TableId tableId) {
        try {
            schema.refresh(connection, tableId, connectorConfig.skipRefreshSchemaOnMissingToastableData());
        }
        catch (SQLException e) {
            throw new ConnectException("Database error while refresing table schema");
        }
    }

    static Optional<DataCollectionSchema> updateSchema(TableId tableId, ChangeRecordEmitter changeRecordEmitter) {
        return ((PostgresChangeRecordEmitter) changeRecordEmitter).newTable(tableId);
    }

    private boolean schemaChanged(List<ReplicationMessage.Column> columns, Table table, boolean metadataInMessage) {
        int tableColumnCount = table.columns().size();
        int replicationColumnCount = columns.size();

        boolean msgHasMissingColumns = tableColumnCount > replicationColumnCount;

        if (msgHasMissingColumns && connectorConfig.skipRefreshSchemaOnMissingToastableData()) {
            // if we are ignoring missing toastable data for the purpose of schema sync, we need to modify the
            // hasMissingColumns boolean to account for this. If there are untoasted columns missing from the replication
            // message, we'll still have missing columns and thus require a schema refresh. However, we can /possibly/
            // avoid the refresh if there are only toastable columns missing from the message.
            msgHasMissingColumns = hasMissingUntoastedColumns(table, columns);
        }

        boolean msgHasAdditionalColumns = tableColumnCount < replicationColumnCount;

        if (msgHasMissingColumns || msgHasAdditionalColumns) {
            // the table metadata has less or more columns than the event, which means the table structure has changed,
            // so we need to trigger a refresh...
            logger.info("Different column count {} present in the server message as schema in memory contains {}; refreshing table schema",
                        replicationColumnCount,
                        tableColumnCount);
            return true;
        }

        // go through the list of columns from the message to figure out if any of them are new or have changed their type based
        // on what we have in the table metadata....
        return columns.stream().filter(message -> {
            String columnName = message.getName();
            Column column = table.columnWithName(columnName);
            if (column == null) {
                logger.info("found new column '{}' present in the server message which is not part of the table metadata; refreshing table schema", columnName);
                return true;
            }
            else {
                final int localType = column.nativeType();
                final int incomingType = message.getType().getOid();
                if (localType != incomingType) {
                    logger.info("detected new type for column '{}', old type was {} ({}), new type is {} ({}); refreshing table schema", columnName, localType, column.typeName(),
                                incomingType, message.getType().getName());
                    return true;
                }
                if (metadataInMessage) {
                    final int localLength = column.length();
                    final int incomingLength = message.getTypeMetadata().getLength();
                    if (localLength != incomingLength) {
                        logger.info("detected new length for column '{}', old length was {}, new length is {}; refreshing table schema", columnName, localLength,
                                    incomingLength);
                        return true;
                    }
                    final int localScale = column.scale().get();
                    final int incomingScale = message.getTypeMetadata().getScale();
                    if (localScale != incomingScale) {
                        logger.info("detected new scale for column '{}', old scale was {}, new scale is {}; refreshing table schema", columnName, localScale,
                                    incomingScale);
                        return true;
                    }
                    final boolean localOptional = column.isOptional();
                    final boolean incomingOptional = message.isOptional();
                    if (localOptional != incomingOptional) {
                        logger.info("detected new optional status for column '{}', old value was {}, new value is {}; refreshing table schema", columnName, localOptional, incomingOptional);
                        return true;
                    }
                }
            }
            return false;
        }).findFirst().isPresent();
    }

    private boolean hasMissingUntoastedColumns(Table table, List<ReplicationMessage.Column> columns) {
        List<String> msgColumnNames = columns.stream()
                .map(ReplicationMessage.Column::getName)
                .collect(Collectors.toList());

        // Compute list of table columns not present in the replication message
        List<String> missingColumnNames = table.columns()
                .stream()
                .filter(c -> !msgColumnNames.contains(c.name()))
                .map(Column::name)
                .collect(Collectors.toList());

        List<String> toastableColumns = schema.getToastableColumnsForTableId(table.id());

        if (logger.isDebugEnabled()) {
            logger.debug("msg columns: '{}' --- missing columns: '{}' --- toastableColumns: '{}",
                    String.join(",", msgColumnNames),
                    String.join(",", missingColumnNames),
                    String.join(",", toastableColumns));
        }
        // Return `true` if we have some columns not in the replication message that are not toastable or that we do
        // not recognize
        return !toastableColumns.containsAll(missingColumnNames);
    }

    private Table tableFromFromMessage(List<ReplicationMessage.Column> columns, Table table) {
        final TableEditor combinedTable = table.edit()
            .setColumns(columns.stream()
                .map(column -> {
                    final PostgresType type = column.getType();
                    final ColumnEditor columnEditor = Column.editor()
                            .name(column.getName())
                            .jdbcType(type.getJdbcId())
                            .type(type.getName())
                            .optional(column.isOptional())
                            .nativeType(type.getOid());
                    columnEditor.length(column.getTypeMetadata().getLength());
                    columnEditor.scale(column.getTypeMetadata().getScale());
                    return columnEditor.create();
                })
                .collect(Collectors.toList())
            );
        final List<String> pkCandidates = table.filterColumnNames(c -> table.isPrimaryKeyColumn(c.name()));
        final Iterator<String> itPkCandidates = pkCandidates.iterator();
        while (itPkCandidates.hasNext()) {
            final String candidateName = itPkCandidates.next();
            if (!combinedTable.hasUniqueValues() && combinedTable.columnWithName(candidateName) == null) {
                logger.error("Potentional inconsistency in key for message {}", columns);
                itPkCandidates.remove();
            }
        }
        combinedTable.setPrimaryKeyNames(pkCandidates);
        return combinedTable.create();
    }

    @Override
    protected boolean skipEmptyMessages() {
        return true;
    }
}
