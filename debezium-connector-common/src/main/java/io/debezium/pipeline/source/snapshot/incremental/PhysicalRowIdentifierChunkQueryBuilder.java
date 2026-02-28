/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

/**
 * Chunk query builder that injects a hidden physical row identifier (e.g. Oracle ROWID, PostgreSQL ctid)
 * so it can be used as the surrogate key for incremental snapshots.
 * <p>
 * This builder is vendor-agnostic at the core level; connector-specific implementations provide
 * the appropriate physical column metadata for their database.
 */
public class PhysicalRowIdentifierChunkQueryBuilder<T extends DataCollectionId> extends AbstractChunkQueryBuilder<T> {

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
                .map(key -> {
                    String unquoted = Strings.unquoteIdentifierPart(key.trim());
                    return Strings.isNullOrEmpty(unquoted) ? key.trim() : unquoted;
                })
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
}
