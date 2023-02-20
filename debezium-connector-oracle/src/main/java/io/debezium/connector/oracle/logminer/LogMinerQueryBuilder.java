/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.Iterator;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.util.Strings;

/**
 * A builder that is responsible for producing the query to be executed against the LogMiner view.
 *
 * @author Chris Cranford
 */
public class LogMinerQueryBuilder {

    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";

    /**
     * Builds the LogMiner contents view query.
     *
     * The returned query will contain 2 bind parameters that the caller is responsible for binding before
     * executing the query.  The first bind parameter is the lower-bounds of the SCN mining window that is
     * not-inclusive while the second is the upper-bounds of the SCN mining window that is inclusive.
     *
     * The built query relies on the following columns from V$LOGMNR_CONTENTS:
     * <pre>
     *     SCN - the system change number at which the change was made
     *     SQL_REDO - the reconstructed SQL statement that initiated the change
     *     OPERATION - the database operation type name
     *     OPERATION_CODE - the database operation numeric code
     *     TIMESTAMP - the time when the change event occurred
     *     XID - the transaction identifier the change participated in
     *     CSF - the continuation flag, identifies rows that should be processed together as single row, 0=no, 1=yes
     *     TABLE_NAME - the name of the table for which the change is for
     *     SEG_OWNER - the name of the schema for which the change is for
     *     USERNAME - the name of the database user that caused the change
     *     ROW_ID - the unique identifier of the row that the change is for, may not always be set with valid value
     *     ROLLBACK - the rollback flag, value of 0 or 1.  1 implies the row was rolled back
     *     RS_ID - the rollback segment idenifier where the change record was record from
     * </pre>
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return the SQL string to be used to fetch changes from Oracle LogMiner
     */
    public static String build(OracleConnectorConfig connectorConfig) {
        final StringBuilder query = new StringBuilder(1024);
        query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, ");
        query.append("USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD# ");
        query.append("FROM ").append(LOGMNR_CONTENTS_VIEW).append(" ");

        // These bind parameters will be bound when the query is executed by the caller.
        query.append("WHERE SCN > ? AND SCN <= ? ");

        // The connector currently requires a "database.pdb.name" configuration property when using CDB mode.
        // If this property is provided, build a predicate that will be used in later predicates.
        final String pdbName = connectorConfig.getPdbName();
        final String pdbPredicate;
        if (!Strings.isNullOrEmpty(pdbName)) {
            // This predicate is used later to explicitly restrict certain OPERATION_CODE and DDL events by the
            // PDB database name while allowing all START, COMMIT, MISSING_SCN, and ROLLBACK operations
            // regardless of where they originate, i.e. the PDB or CDB$ROOT.
            pdbPredicate = "SRC_CON_NAME = '" + pdbName + "' ";
        }
        else {
            pdbPredicate = null;
        }

        // Excluded schemas, if defined
        // This prevents things such as picking DDL for changes to LogMiner tables in SYSTEM tablespace
        // or picking up DML changes inside the SYS and SYSTEM tablespaces.
        final String excludedSchemas = resolveExcludedSchemaPredicate("SEG_OWNER");
        if (excludedSchemas.length() > 0) {
            query.append("AND ").append(excludedSchemas).append(' ');
        }

        // Add the operation types we're interested in.
        if (!Strings.isNullOrEmpty(pdbPredicate)) {
            query.append("AND (OPERATION_CODE IN (6,7,36) ");
            query.append("OR (").append(pdbPredicate);
            query.append("AND OPERATION_CODE IN (1,2,3,5");
            if (connectorConfig.isLobEnabled()) {
                query.append(",9,10,11,29");
            }
            query.append(",34,255)");
            query.append("))");
        }
        else {
            query.append("AND OPERATION_CODE IN (1,2,3,5,6,7");
            if (connectorConfig.isLobEnabled()) {
                query.append(",9,10,11,29");
            }
            query.append(",34,36,255)");
        }

        return query.toString();
    }

    /**
     * Resolve the built-in excluded schemas predicate.
     *
     * @param fieldName the query field name the predicate applies to, should never be {@code null}
     * @return the predicate
     */
    private static String resolveExcludedSchemaPredicate(String fieldName) {
        // There are some common schemas that we automatically ignore when building the runtime Filter
        // predicates, and we put that same list of schemas here and apply those in the generated SQL.
        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            StringBuilder query = new StringBuilder();
            query.append('(').append(fieldName).append(" IS NULL OR ");
            query.append(fieldName).append(" NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                query.append('\'').append(excludedSchema.toUpperCase()).append('\'');
                if (i.hasNext()) {
                    query.append(',');
                }
            }
            return query.append(')').append(')').toString();
        }
        return "";
    }
}
