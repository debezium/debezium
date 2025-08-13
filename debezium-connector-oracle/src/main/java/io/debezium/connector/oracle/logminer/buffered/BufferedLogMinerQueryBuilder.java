/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.AbstractLogMinerQueryBuilder;

/**
 * A builder that is responsible for producing the query to be executed against LogMiner when operating
 * in buffered mode, where redo logs are read without using {@code COMMITTED_DATA_ONLY} mode.
 *
 * @author Chris Cranford
 */
public class BufferedLogMinerQueryBuilder extends AbstractLogMinerQueryBuilder {

    private static final List<Integer> OPERATION_CODES_LOB = Arrays.asList(1, 2, 3, 6, 7, 9, 10, 11, 27, 29, 34, 36, 68, 70, 71, 91, 92, 93, 255);
    private static final List<Integer> OPERATION_CODES_NO_LOB = Arrays.asList(1, 2, 3, 6, 7, 27, 34, 36, 255);

    // CTE query excludes START,COMMIT,ROLLBACK markers - only interested in non-transaction marker changes
    private static final List<Integer> CTE_OPERATION_CODES_LOB = Arrays.asList(1, 2, 3, 9, 10, 11, 27, 29, 34, 68, 70, 71, 91, 92, 93, 255);
    private static final List<Integer> CTE_OPERATION_CODES_NO_LOB = Arrays.asList(1, 2, 3, 27, 34, 255);

    public BufferedLogMinerQueryBuilder(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected String getPredicates(boolean isCteQuery) {
        final StringBuilder query = new StringBuilder(1024);

        // These bind parameters will be bound when the query is executed by the caller.
        query.append("SCN > ? AND SCN <= ?");

        // Transactions cannot span across multiple pluggable database boundaries.
        // Therefore, it is safe to apply the multi-tenant restriction to the full query when
        // the connector is configured to connector to a multi-tenant environment.
        final String multiTenantPredicate = getMultiTenantPredicate();
        if (!multiTenantPredicate.isEmpty()) {
            query.append(" AND ").append(multiTenantPredicate);
        }

        query.append(" AND ");
        if (!connectorConfig.storeOnlyCapturedTables()) {
            query.append("((");
        }

        // Operations predicate
        // This predicate never returns EMPTY; so inline directly.
        query.append(getOperationCodePredicate(isCteQuery));

        // Include/Exclude usernames
        final String userNamePredicate = getUserNamePredicate();
        if (!userNamePredicate.isEmpty()) {
            query.append(" AND ").append(userNamePredicate);
        }

        // Include/Exclude clients
        final String clientIdPredicate = getClientIdPredicate();
        if (!clientIdPredicate.isEmpty()) {
            query.append(" AND ").append(clientIdPredicate);
        }

        // Generate the schema-based predicates
        final String schemasPredicate = getSchemaNamePredicate();
        if (!schemasPredicate.isEmpty()) {
            query.append(" AND ").append(schemasPredicate);
        }

        // Generate the table-based predicates
        final String tablesPredicate = getTableNamePredicate();
        if (!tablesPredicate.isEmpty()) {
            query.append(" AND ").append(tablesPredicate);
        }

        if (!connectorConfig.storeOnlyCapturedTables()) {
            query.append(")").append(getDdlPredicate()).append(")");
        }

        return query.toString();
    }

    /**
     * Get the redo entry operation code predicate.
     *
     * @param isCteQuery whether to build the operation code predicate for CTE
     * @return operation code predicate, never {@code null} nor empty.
     */
    private String getOperationCodePredicate(boolean isCteQuery) {
        // This predicate excepts that if we are limiting data to a pluggable database (PDB) that another
        // predicate has been applied to restrict the operations to only that PDB. This predicate will be
        // generated to capture the following operations as a baseline:
        //
        // INSERT (1), UPDATE (2), DELETE (3),
        // START (6), COMMIT (7), ROLLBACK (36),
        // MISSING_SCN (34), UNSUPPORTED (255)
        //
        // When the connector is configured to capture LOB operations, the connector will also
        // capture operations related to LOB columns, those operations are:
        //
        // SElECT_LOB_LOCATOR (9), LOB_WRITE (10), LOB_WRITE (11), LOB_ERASE (29)
        //
        final StringBuilder predicate = new StringBuilder();

        // Handle all operations except DDL changes
        final InClause operationInClause = InClause.builder().withField("OPERATION_CODE");
        operationInClause.withValues(getOperationCodesList(isCteQuery));
        predicate.append("(").append(operationInClause.build());

        // Handle DDL operations
        if (connectorConfig.storeOnlyCapturedTables()) {
            predicate.append(getDdlPredicate());
        }

        return predicate.append(")").toString();
    }

    private static String getDdlPredicate() {
        return " OR (OPERATION_CODE = 5 AND INFO NOT LIKE 'INTERNAL DDL%')";
    }

    private List<Integer> getOperationCodesList(boolean isCteQuery) {
        if (connectorConfig.isLobEnabled()) {
            return isCteQuery ? CTE_OPERATION_CODES_LOB : OPERATION_CODES_LOB;
        }

        final List<Integer> nonLobOperations = isCteQuery ? CTE_OPERATION_CODES_NO_LOB : OPERATION_CODES_NO_LOB;

        if (connectorConfig.isLegacyLogMinerHeapTransactionStartBehaviorEnabled()) {
            // The legacy behavior skipped START events as a performance optimization to avoid adding
            // extra objects to the transaction cache. Without these, the username and client id
            // filter options and source information block fields won't work in some corner cases if
            // the transaction start is in a prior archive log.
            final List<Integer> operationCodes = new ArrayList<>(nonLobOperations);
            operationCodes.removeIf(operationCode -> operationCode == 6);
            return operationCodes;
        }

        return nonLobOperations;
    }
}
