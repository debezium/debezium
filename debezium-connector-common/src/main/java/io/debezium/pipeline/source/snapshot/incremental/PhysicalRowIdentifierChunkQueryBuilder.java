/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.CascadingOrBoundaryConditions;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

/**
 * Chunk query builder that injects a physical row identifier such as Oracle ROWID.
 * <p>
 * Connector-specific implementations supply the physical column metadata.
 */
public class PhysicalRowIdentifierChunkQueryBuilder<T extends DataCollectionId> extends AbstractChunkQueryBuilder<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalRowIdentifierChunkQueryBuilder.class);
    private static final String DEFAULT_TABLE_ALIAS = "DBZ_PHYSICAL_ROW";

    private final String physicalColumnName;
    private final String physicalIdentifierExpression;
    private final int physicalJdbcType;
    private final int physicalNativeType;
    private final String physicalTypeName;
    private final Integer physicalLength;
    private final Integer physicalScale;
    private final boolean aliasWildcardProjection;
    private final String tableAlias;

    private Object prefetchedChunkEndValue;

    public PhysicalRowIdentifierChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                                  JdbcConnection jdbcConnection,
                                                  String physicalColumnName,
                                                  String physicalIdentifierExpression,
                                                  int physicalJdbcType,
                                                  int physicalNativeType,
                                                  String physicalTypeName,
                                                  Integer physicalLength,
                                                  Integer physicalScale,
                                                  boolean aliasWildcardProjection,
                                                  String tableAlias) {
        super(config, jdbcConnection);
        this.physicalColumnName = physicalColumnName;
        this.physicalIdentifierExpression = physicalIdentifierExpression;
        this.physicalJdbcType = physicalJdbcType;
        this.physicalNativeType = physicalNativeType;
        this.physicalTypeName = physicalTypeName;
        this.physicalLength = physicalLength;
        this.physicalScale = physicalScale;
        this.aliasWildcardProjection = aliasWildcardProjection;
        this.tableAlias = aliasWildcardProjection ? Strings.defaultIfEmpty(tableAlias, DEFAULT_TABLE_ALIAS) : null;
    }

    @Override
    public Table prepareTable(IncrementalSnapshotContext<T> context, Table table) {
        if (table == null || table.columnWithName(physicalColumnName) != null || !usesPhysicalIdentifier(context)) {
            return table;
        }
        TableEditor editor = table.edit();
        editor.addColumn(buildPhysicalColumn(table));
        return editor.create();
    }

    private Column buildPhysicalColumn(Table table) {
        ColumnEditor columnEditor = Column.editor()
                .name(physicalColumnName)
                .jdbcType(physicalJdbcType)
                .nativeType(physicalNativeType)
                .type(physicalTypeName)
                .optional(false)
                .position(table.columns().size() + 1);
        if (physicalLength != null) {
            columnEditor.length(physicalLength);
        }
        if (physicalScale != null) {
            columnEditor.scale(physicalScale);
        }
        return columnEditor.create();
    }

    private boolean usesPhysicalIdentifier(IncrementalSnapshotContext<T> context) {
        if (context == null || context.currentDataCollectionId() == null) {
            return false;
        }
        return context.currentDataCollectionId().getSurrogateKey()
                .map(String::trim)
                .map(key -> Strings.defaultIfEmpty(Strings.unquoteIdentifierPart(key), key))
                .filter(physicalColumnName::equalsIgnoreCase)
                .isPresent();
    }

    private boolean hasPhysicalColumn(Table table) {
        return table != null && table.columnWithName(physicalColumnName) != null;
    }

    @Override
    protected String buildProjection(Table table) {
        if (!hasPhysicalColumn(table)) {
            return super.buildProjection(table);
        }
        final String baseProjection = super.buildProjection(table);
        final String physicalProjection = buildPhysicalProjection();
        if ("*".equals(baseProjection)) {
            if (aliasWildcardProjection && !Strings.isNullOrEmpty(tableAlias)) {
                return physicalProjection + ", " + tableAlias + ".*";
            }
            return physicalProjection + ", *";
        }
        final String quotedColumn = jdbcConnection.quoteIdentifier(physicalColumnName);
        String filteredBaseProjection = Arrays.stream(baseProjection.split(","))
                .map(String::trim)
                .filter(token -> !token.equals(quotedColumn))
                .collect(Collectors.joining(", "));
        if (filteredBaseProjection.isEmpty()) {
            return physicalProjection;
        }
        return physicalProjection + ", " + filteredBaseProjection;
    }

    private String buildPhysicalProjection() {
        return physicalIdentifierExpression + " AS " + jdbcConnection.quoteIdentifier(physicalColumnName);
    }

    @Override
    protected Optional<String> getTableAlias(Table table) {
        if (aliasWildcardProjection && hasPhysicalColumn(table)) {
            return Optional.ofNullable(tableAlias);
        }
        return Optional.empty();
    }

    /**
     * Prefetches the next physical-identifier boundary before building the chunk query.
     */
    @Override
    public String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, int limit, Optional<String> additionalCondition) {
        if (limit > 0 && usesPhysicalIdentifier(context)) {
            prefetchedChunkEndValue = executePrefetch(context, table, limit, additionalCondition);
            LOGGER.debug("Prefetched chunk-end boundary: {}", prefetchedChunkEndValue);
        }
        else {
            prefetchedChunkEndValue = null;
        }
        return super.buildChunkQuery(context, table, limit, additionalCondition);
    }

    /**
     * Returns the prefetched physical-identifier boundary, falling back to the table maximum on the last chunk.
     */
    @Override
    protected Optional<Object[]> getChunkUpperBound(IncrementalSnapshotContext<T> context) {
        if (!usesPhysicalIdentifier(context)) {
            return super.getChunkUpperBound(context);
        }
        if (prefetchedChunkEndValue != null) {
            return Optional.of(new Object[]{ prefetchedChunkEndValue });
        }
        return context.maximumKey();
    }

    /**
     * Orders by primary key when the physical row identifier is active.
     */
    @Override
    protected List<Column> getOrderByColumns(IncrementalSnapshotContext<T> context, Table table) {
        if (usesPhysicalIdentifier(context)) {
            List<Column> pkColumns = getKeyMapper().getKeyKolumns(table);
            if (!pkColumns.isEmpty()) {
                return pkColumns;
            }
        }
        return super.getOrderByColumns(context, table);
    }

    /**
     * Returns the prefetched boundary as the next chunk start, falling back to the table maximum when no prefetch value is available.
     */
    @Override
    public Object[] resolveChunkEndPosition(IncrementalSnapshotContext<T> context, Table table, Object[] lastRowKey) {
        if (prefetchedChunkEndValue != null) {
            return new Object[]{ prefetchedChunkEndValue };
        }
        if (usesPhysicalIdentifier(context) && context.maximumKey().isPresent()) {
            return context.maximumKey().get();
        }
        return lastRowKey;
    }

    /**
     * Queries for the physical-identifier value at the {@code limit}-th row from the current chunk start.
     *
     * @return the boundary value, or {@code null} when fewer rows remain
     */
    private Object executePrefetch(IncrementalSnapshotContext<T> context, Table table, int limit, Optional<String> additionalCondition) {
        final String sql = buildPrefetchQuery(context, table, limit, additionalCondition);

        LOGGER.debug("Prefetch chunk-end query: {}", sql);

        try (PreparedStatement stmt = jdbcConnection.connection().prepareStatement(sql)) {
            if (context.isNonInitialChunk()) {
                CascadingOrBoundaryConditions.bindTriangularParamsSkipNulls(stmt, getQueryColumns(context, table), context.chunkEndPosititon(), 1, jdbcConnection);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
                return null;
            }
        }
        catch (SQLException e) {
            LOGGER.error("Failed to prefetch chunk-end boundary for table {}", table.id(), e);
            throw new DebeziumException("Failed to determine chunk end position via prefetch query for table " + table.id(), e);
        }
    }

    protected String buildPrefetchQuery(IncrementalSnapshotContext<T> context, Table table, int limit, Optional<String> additionalCondition) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(physicalIdentifierExpression);
        sql.append(" FROM ");
        sql.append(buildTableReference(table));

        boolean hasWhere = false;
        if (context.isNonInitialChunk()) {
            sql.append(" WHERE ");
            addLowerBound(context, table, context.chunkEndPosititon(), sql);
            hasWhere = true;
        }
        if (additionalCondition.isPresent()) {
            sql.append(hasWhere ? " AND " : " WHERE ");
            sql.append(additionalCondition.get());
        }

        sql.append(" ORDER BY ").append(getPrefetchOrderByExpression());
        sql.append(" OFFSET ").append(limit - 1);
        sql.append(" ROWS FETCH NEXT 1 ROWS ONLY");

        return sql.toString();
    }

    protected String getPrefetchOrderByExpression() {
        return physicalIdentifierExpression;
    }

    /**
     * Builds a quoted table reference for {@code FROM} clauses.
     */
    protected String buildTableReference(Table table) {
        return jdbcConnection.quotedTableIdString(table.id());
    }
}
