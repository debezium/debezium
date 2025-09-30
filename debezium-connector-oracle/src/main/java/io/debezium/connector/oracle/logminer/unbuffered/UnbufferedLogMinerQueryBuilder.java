/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.AbstractLogMinerQueryBuilder;
import io.debezium.util.Strings;

/**
 * Builder that creates an Oracle LogMiner query for the unbuffered adapter implementation.
 *
 * @author Chris Cranford
 */
@Incubating
public class UnbufferedLogMinerQueryBuilder extends AbstractLogMinerQueryBuilder {

    public UnbufferedLogMinerQueryBuilder(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected String getPredicates(boolean isCteQuery) {
        final StringBuilder predicates = new StringBuilder(1024);

        // For DDL operations, the COMMIT_SCN column is NULL when using COMMITTED_DATA_ONLY mode,
        // and so we use the INFO field combined with the COMMIT_SCN field to retrieve those
        // specific rows.
        predicates.append("(COMMIT_SCN >= ? AND COMMIT_SCN < ? OR (COMMIT_SCN IS NULL AND INFO IN (' DDL', 'USER DDL (PlSql=0 RecDep=0)')))");

        final String multiTenantPredicate = getMultiTenantPredicate();
        if (!Strings.isNullOrEmpty(multiTenantPredicate)) {
            predicates.append(" AND ").append(multiTenantPredicate);
        }

        predicates.append(" AND OPERATION_CODE IN (").append(getOperationCodes(isCteQuery)).append(")");

        final String userNamePredicate = getUserNamePredicate();
        if (!Strings.isNullOrEmpty(userNamePredicate)) {
            predicates.append(" AND ").append(userNamePredicate);
        }

        final String clientIdPredicate = getClientIdPredicate();
        if (!Strings.isNullOrEmpty(clientIdPredicate)) {
            predicates.append(" AND ").append(clientIdPredicate);
        }

        final String tableNamePredicate = getTableNamePredicate();
        if (!Strings.isNullOrEmpty(tableNamePredicate)) {
            predicates.append(" AND ").append(tableNamePredicate);
        }

        final String schemaNamePredicate = getSchemaNamePredicate();
        if (!Strings.isNullOrEmpty(schemaNamePredicate)) {
            predicates.append(" AND ").append(schemaNamePredicate);
        }

        return predicates.toString();
    }

    private String getOperationCodes(boolean isCteQuery) {
        // For CTE queries we exclude START=6,COMMIT=7,ROLLBACK=36 - only interested in non-transaction markers
        if (connectorConfig.isLobEnabled()) {
            if (isCteQuery) {
                return "1,2,3,5,9,10,11,27,29,34,68,70,71,91,92,93,255";
            }
            return "1,2,3,5,6,7,9,10,11,27,29,34,36,68,70,71,91,92,93,255";
        }
        else {
            if (isCteQuery) {
                return "1,2,3,5,27,34,255";
            }
            return "1,2,3,5,6,7,27,34,36,255";
        }
    }

}
