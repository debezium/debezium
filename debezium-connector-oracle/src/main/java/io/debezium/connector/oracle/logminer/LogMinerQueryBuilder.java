/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
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
     * @param schema database schema, should not be {@code null}
     * @return the SQL string to be used to fetch changes from Oracle LogMiner
     */
    public static String build(OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema) {
        final StringBuilder query = new StringBuilder(1024);
        query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, ");
        query.append("USERNAME, ROW_ID, ROLLBACK, RS_ID ");
        query.append("FROM ").append(LOGMNR_CONTENTS_VIEW).append(" ");

        // These bind parameters will be bound when the query is executed by the caller.
        query.append("WHERE SCN > ? AND SCN <= ? ");

        // Restrict to configured PDB if one is supplied
        final String pdbName = connectorConfig.getPdbName();
        if (!Strings.isNullOrEmpty(pdbName)) {
            query.append("AND ").append("SRC_CON_NAME = '").append(pdbName.toUpperCase()).append("' ");
        }

        query.append("AND (");

        // Always include START, COMMIT, MISSING_SCN, and ROLLBACK operations
        query.append("(OPERATION_CODE IN (6,7,34,36)");

        if (!schema.storeOnlyCapturedTables()) {
            // In this mode, the connector will always be fed DDL operations for all tables even if they
            // are not part of the inclusion/exclusion lists.
            query.append(" OR ").append(buildDdlPredicate()).append(" ");
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (connectorConfig.isLobEnabled()) {
                query.append(") OR (OPERATION_CODE IN (1,2,3,9,10,11,29) ");
            }
            else {
                query.append(") OR (OPERATION_CODE IN (1,2,3) ");
            }
        }
        else {
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (connectorConfig.isLobEnabled()) {
                query.append(") OR ((OPERATION_CODE IN (1,2,3,9,10,11,29) ");
            }
            else {
                query.append(") OR ((OPERATION_CODE IN (1,2,3) ");
            }
            // In this mode, the connector will filter DDL operations based on the table inclusion/exclusion lists
            query.append("OR ").append(buildDdlPredicate()).append(") ");
        }

        // Always ignore the flush table
        query.append("AND TABLE_NAME != '").append(LogWriterFlushStrategy.LOGMNR_FLUSH_TABLE).append("' ");

        // There are some common schemas that we automatically ignore when building the runtime Filter
        // predicates and we put that same list of schemas here and apply those in the generated SQL.
        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            query.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                query.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(") ");
        }

        String schemaPredicate = buildSchemaPredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            query.append("AND ").append(schemaPredicate).append(" ");
        }

        String tablePredicate = buildTablePredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            query.append("AND ").append(tablePredicate).append(" ");
        }

        query.append("))");

        Set<String> excludedUsers = connectorConfig.getLogMiningUsernameExcludes();
        if (!excludedUsers.isEmpty()) {
            query.append(" AND USERNAME NOT IN (");
            for (Iterator<String> i = excludedUsers.iterator(); i.hasNext();) {
                String user = i.next();
                query.append("'").append(user).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(")");
        }

        return query.toString();
    }

    /**
     * Builds a common SQL fragment used to obtain DDL operations via LogMiner.
     *
     * @return predicate that can be used to obtain DDL operations via LogMiner
     */
    private static String buildDdlPredicate() {
        final StringBuilder predicate = new StringBuilder(256);
        predicate.append("(OPERATION_CODE = 5 ");
        predicate.append("AND USERNAME NOT IN ('SYS','SYSTEM') ");
        predicate.append("AND INFO NOT LIKE 'INTERNAL DDL%' ");
        predicate.append("AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%'))");
        return predicate.toString();
    }

    /**
     * Builds a SQL predicate of what schemas to include/exclude based on the connector configuration.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return SQL predicate to filter results based on schema include/exclude configurations
     */
    private static String buildSchemaPredicate(OracleConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.schemaIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.schemaExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER", true)).append(")");
            }
        }
        else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER", false)).append(")");
        }
        return predicate.toString();
    }

    /**
     * Builds a SQL predicate of what tables to include/exclude based on the connector configuration.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return SQL predicate to filter results based on table include/exclude configuration
     */
    private static String buildTablePredicate(OracleConnectorConfig connectorConfig) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.tableIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.tableExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", true)).append(")");
            }
        }
        else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, "SEG_OWNER || '.' || TABLE_NAME", false)).append(")");
        }
        return predicate.toString();
    }

    /**
     * Takes a list of reg-ex patterns and builds an Oracle-specific predicate using {@code REGEXP_LIKE}
     * in order to take the connector configuration include/exclude lists and assemble them as SQL
     * predicates.
     *
     * @param patterns list of each individual include/exclude reg-ex patterns from connector configuration
     * @param columnName the column in which the reg-ex patterns are to be applied against
     * @param inclusion should be {@code true} when passing inclusion patterns, {@code false} otherwise
     * @return
     */
    private static String listOfPatternsToSql(List<Pattern> patterns, String columnName, boolean inclusion) {
        StringBuilder predicate = new StringBuilder();
        for (Iterator<Pattern> i = patterns.iterator(); i.hasNext();) {
            Pattern pattern = i.next();
            if (inclusion) {
                predicate.append("NOT ");
            }
            // NOTE: The REGEXP_LIKE operator was added in Oracle 10g (10.1.0.0.0)
            final String text = resolveRegExpLikePattern(pattern);
            predicate.append("REGEXP_LIKE(").append(columnName).append(",'").append(text).append("','i')");
            if (i.hasNext()) {
                // Exclude lists imply combining them via AND, Include lists imply combining them via OR?
                predicate.append(inclusion ? " AND " : " OR ");
            }
        }
        return predicate.toString();
    }

    /**
     * The {@code REGEXP_LIKE} Oracle operator acts identical to the {@code LIKE} operator. Internally,
     * it prepends and appends a "%" qualifier.  The include/exclude lists are meant to be explicit in
     * that they have an implied "^" and "$" qualifier for start/end so that the LIKE operation does
     * not mistakently filter "DEBEZIUM2" when using the reg-ex of "DEBEZIUM".
     *
     * @param pattern the pattern to be analyzed, should not be {@code null}
     * @return the adjusted predicate, if necessary and doesn't already explicitly specify "^" or "$"
     */
    private static String resolveRegExpLikePattern(Pattern pattern) {
        String text = pattern.pattern();
        if (!text.startsWith("^")) {
            text = "^" + text;
        }
        if (!text.endsWith("$")) {
            text += "$";
        }
        return text;
    }
}
