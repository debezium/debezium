/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningQueryFilterMode;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * A builder that is responsible for producing the query to be executed against the LogMiner view.
 *
 * @author Chris Cranford
 */
public class LogMinerQueryBuilder {

    private static final String EMPTY = "";
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";
    private static final String UNKNOWN_USERNAME = "UNKNOWN";
    private static final String UNKNOWN_SCHEMA_NAME = "UNKNOWN";
    private static final String UNKNOWN_TABLE_NAME_PREFIX = "OBJ#";
    private static final List<Integer> OPERATION_CODES_LOB = Arrays.asList(1, 2, 3, 6, 7, 9, 10, 11, 29, 34, 36, 68, 70, 71, 255);
    private static final List<Integer> OPERATION_CODES_NO_LOB = Arrays.asList(1, 2, 3, 6, 7, 34, 36, 255);

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
        query.append("WHERE SCN > ? AND SCN <= ?");

        // Transactions cannot span across multiple pluggable database boundaries.
        // Therefore, it is safe to apply the multi-tenant restriction to the full query when
        // the connector is configured to connector to a multi-tenant environment.
        final String multiTenantPredicate = getMultiTenantPredicate(connectorConfig);
        if (!multiTenantPredicate.isEmpty()) {
            query.append(" AND ").append(multiTenantPredicate);
        }

        query.append(" AND ");
        if (!connectorConfig.storeOnlyCapturedTables()) {
            query.append("((");
        }

        // Operations predicate
        // This predicate never returns EMPTY; so inline directly.
        query.append(getOperationCodePredicate(connectorConfig));

        // Include/Exclude usernames
        final String userNamePredicate = getUserNamePredicate(connectorConfig);
        if (!userNamePredicate.isEmpty()) {
            query.append(" AND ").append(userNamePredicate);
        }

        // Generate the schema-based predicates
        final String schemasPredicate = getSchemaNamePredicate(connectorConfig);
        if (!schemasPredicate.isEmpty()) {
            query.append(" AND ").append(schemasPredicate);
        }

        // Generate the table-based predicates
        final String tablesPredicate = getTableNamePredicate(connectorConfig);
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
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return operation code predicate, never {@code null} nor {@link #EMPTY}.
     */
    private static String getOperationCodePredicate(OracleConnectorConfig connectorConfig) {
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
        if (connectorConfig.isLobEnabled()) {
            operationInClause.withValues(OPERATION_CODES_LOB);
        }
        else {
            final List<Integer> operationCodes = new ArrayList<>(OPERATION_CODES_NO_LOB);
            // The transaction start event needs to be handled when a persistent buffer (Infinispan) is used
            // because it is needed to reset the event id counter when re-mining transaction events.
            if (connectorConfig.getLogMiningBufferType() == OracleConnectorConfig.LogMiningBufferType.MEMORY) {
                operationCodes.removeIf(operationCode -> operationCode == 6);
            }
            operationInClause.withValues(operationCodes);
        }
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

    /**
     * Get the multi-tenant predicate based on the configured pluggable database name.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return multi-tenant predicate, may be an empty string if multi-tenancy is not enabled
     */
    private static String getMultiTenantPredicate(OracleConnectorConfig connectorConfig) {
        if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
            return "SRC_CON_NAME = '" + connectorConfig.getPdbName() + "'";
        }
        return EMPTY;
    }

    /**
     * Generate a username based predicate for include/exclude usernames.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return the username predicate, may be an empty string if no predicate is generated
     */
    private static String getUserNamePredicate(OracleConnectorConfig connectorConfig) {
        if (!LogMiningQueryFilterMode.NONE.equals(connectorConfig.getLogMiningQueryFilterMode())) {
            // Only filter usernames when using IN and REGEX modes
            // Username filters always use an IN-clause predicate
            //
            // Oracle always stores usernames as upper-case.
            // This predicate build applies upper-case to the provided value lists from the configuration.
            return IncludeExcludeInClause.builder()
                    .withField("USERNAME")
                    .withFilterMode(LogMiningQueryFilterMode.IN)
                    .withDefaultIncludeValues(Collections.singletonList(UNKNOWN_USERNAME))
                    .withIncludeValues(connectorConfig.getLogMiningUsernameIncludes())
                    .withExcludeValues(connectorConfig.getLogMiningUsernameExcludes())
                    .caseInsensitive()
                    .build();
        }
        return EMPTY;
    }

    /**
     * Get the schema-based predicate.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return the schema predicate, may be an empty string if no predicate is generated
     */
    private static String getSchemaNamePredicate(OracleConnectorConfig connectorConfig) {
        // The name of the column in the V$LOGMNR_CONTENTS view that represents the schema name.
        final String fieldName = "SEG_OWNER";

        final String includeList = connectorConfig.schemaIncludeList();
        final String excludeList = connectorConfig.schemaExcludeList();

        final LogMiningQueryFilterMode queryFilterMode = connectorConfig.getLogMiningQueryFilterMode();
        if (LogMiningQueryFilterMode.NONE.equals(queryFilterMode)) {
            // Only apply the hard-coded schema exclusions
            return resolveExcludedSchemaPredicate(fieldName);
        }
        else if (Strings.isNullOrEmpty(includeList) && Strings.isNullOrEmpty(excludeList)) {
            // Only apply the hard-coded schema exclusions
            return resolveExcludedSchemaPredicate(fieldName);
        }
        else if (LogMiningQueryFilterMode.IN.equals(queryFilterMode)) {
            final Set<String> includeSchemasList = Strings.setOfTrimmed(includeList, String::new);
            final Set<String> excludeSchemasList = Strings.setOfTrimmed(excludeList, String::new);

            final StringBuilder predicate = new StringBuilder();
            predicate.append("(").append(fieldName).append(" IS NULL OR ");

            predicate.append(IncludeExcludeInClause.builder()
                    .withField(fieldName)
                    .withFilterMode(queryFilterMode)
                    .withIncludeValues(includeSchemasList)
                    .withDefaultIncludeValues(Collections.singletonList(UNKNOWN_SCHEMA_NAME))
                    .withExcludeValues(excludeSchemasList)
                    .withDefaultExcludeValues(getBuiltInExcludedSchemas())
                    .caseInsensitive()
                    .build());
            predicate.append(")");
            return predicate.toString();
        }
        else if (LogMiningQueryFilterMode.REGEX.equals(queryFilterMode)) {
            final List<Pattern> includeSchemaList = Strings.listOfRegex(includeList, Pattern.CASE_INSENSITIVE);
            final List<Pattern> excludeSchemaList = Strings.listOfRegex(excludeList, Pattern.CASE_INSENSITIVE);

            final StringBuilder predicate = new StringBuilder();
            predicate.append("(");
            predicate.append(fieldName).append(" IS NULL OR ");

            if (includeSchemaList.isEmpty() && !excludeSchemaList.isEmpty()) {
                // Only exclusions are applied
                // This special case will apply built-in schema exclusions using IN-clause
                predicate.append(InClause.builder()
                        .withField(fieldName)
                        .negate()
                        .withValues(getBuiltInExcludedSchemas())
                        .build());
            }
            else {
                // Include filters are specified
                predicate.append(fieldName).append(" = '").append(UNKNOWN_SCHEMA_NAME).append("'");
            }

            predicate.append(" OR ");
            predicate.append(IncludeExcludeRegExpLike.builder()
                    .withField(fieldName)
                    .withFilterMode(queryFilterMode)
                    .withIncludeValues(includeSchemaList)
                    .withExcludeValues(excludeSchemaList)
                    .build());

            predicate.append(")");

            return predicate.toString();
        }
        else {
            throw new DebeziumException("An unsupported LogMiningQueryFilterMode detected: " + queryFilterMode);
        }
    }

    private static String getTableNamePredicate(OracleConnectorConfig connectorConfig) {
        // The name of the column in the V$LOGMNR_CONTENTS view that represents the schema and table names.
        final String fieldName = "SEG_OWNER || '.' || TABLE_NAME";

        final String includeList = connectorConfig.tableIncludeList();
        final String excludeList = connectorConfig.tableExcludeList();

        final LogMiningQueryFilterMode queryFilterMode = connectorConfig.getLogMiningQueryFilterMode();
        if (LogMiningQueryFilterMode.NONE.equals(queryFilterMode)) {
            // No filters get applied
            return EMPTY;
        }
        else if (Strings.isNullOrEmpty(includeList) && Strings.isNullOrEmpty(excludeList)) {
            // No table filters provided, nothing to apply
            return EMPTY;
        }
        else if (LogMiningQueryFilterMode.IN.equals(queryFilterMode)) {
            final List<String> includeTableList = getTableIncludeExcludeListAsInValueList(includeList);
            final List<String> excludeTableList = getTableIncludeExcludeListAsInValueList(excludeList);
            final StringBuilder predicate = new StringBuilder();
            // Makes sure we get rows that have no TABLE_NAME or that have had an issue resolving the
            // table's object identifier due to a recent schema change causing a dictionary mismatch.
            predicate.append("(TABLE_NAME IS NULL OR ");
            predicate.append("TABLE_NAME LIKE '").append(UNKNOWN_TABLE_NAME_PREFIX).append("%' OR ");

            getSignalDataCollectionId(connectorConfig).ifPresent(signalTableId -> {
                if (!tableIncludeListContains(includeTableList, signalTableId)) {
                    predicate.append("UPPER(")
                            .append(fieldName)
                            .append(") = '")
                            .append(signalTableId.schema().toUpperCase())
                            .append('.')
                            .append(signalTableId.table().toUpperCase())
                            .append("' OR ");
                }
            });

            predicate.append(IncludeExcludeInClause.builder()
                    .withField(fieldName)
                    .withFilterMode(queryFilterMode)
                    .withIncludeValues(includeTableList)
                    .withExcludeValues(excludeTableList)
                    .caseInsensitive()
                    .build());
            predicate.append(")");
            return predicate.toString();
        }
        else if (LogMiningQueryFilterMode.REGEX.equals(queryFilterMode)) {
            final List<Pattern> includeTableList = Strings.listOfRegex(includeList, Pattern.CASE_INSENSITIVE);
            final List<Pattern> excludeTableList = Strings.listOfRegex(excludeList, Pattern.CASE_INSENSITIVE);
            final StringBuilder predicate = new StringBuilder();
            // Makes sure we get rows that have no TABLE_NAME or that have had an issue resolving the
            // table's object identifier due to a recent schema change causing a dictionary mismatch.
            predicate.append("(TABLE_NAME IS NULL OR ");
            predicate.append("TABLE_NAME LIKE '").append(UNKNOWN_TABLE_NAME_PREFIX).append("%' OR ");

            getSignalDataCollectionId(connectorConfig).ifPresent(signalTableId -> {
                if (!matches(includeTableList, signalTableId.identifier())) {
                    predicate.append("UPPER(")
                            .append(fieldName)
                            .append(") = '")
                            .append(signalTableId.schema().toUpperCase())
                            .append('.')
                            .append(signalTableId.table().toUpperCase())
                            .append("' OR ");
                }
            });

            predicate.append(IncludeExcludeRegExpLike.builder()
                    .withField(fieldName)
                    .withFilterMode(queryFilterMode)
                    .withIncludeValues(includeTableList)
                    .withExcludeValues(excludeTableList)
                    .build());
            predicate.append(")");
            return predicate.toString();
        }
        else {
            throw new DebeziumException("An unsupported LogMiningQueryFilterMode detected: " + queryFilterMode);
        }
    }

    private static Optional<TableId> getSignalDataCollectionId(OracleConnectorConfig connectorConfig) {
        if (!Strings.isNullOrEmpty(connectorConfig.getSignalingDataCollectionId())) {
            return Optional.of(TableId.parse(connectorConfig.getSignalingDataCollectionId()));
        }
        return Optional.empty();
    }

    private static boolean tableIncludeListContains(Collection<String> values, TableId searchValue) {
        for (String value : values) {
            final TableId tableId = TableId.parse(value, false);
            if (tableId.schema().equalsIgnoreCase(searchValue.schema()) &&
                    tableId.table().equalsIgnoreCase(searchValue.table())) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(Collection<Pattern> patterns, String searchValue) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(searchValue).matches()) {
                return true;
            }
        }
        return false;
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
        final List<String> schemas = getBuiltInExcludedSchemas();
        if (!schemas.isEmpty()) {
            StringBuilder query = new StringBuilder();
            query.append('(').append(fieldName).append(" IS NULL OR ");
            query.append(InClause.builder().withField(fieldName).negate().withValues(schemas).build());
            query.append(')');
            return query.toString();
        }
        return EMPTY;
    }

    private static List<String> getBuiltInExcludedSchemas() {
        return toUpperCase(OracleConnectorConfig.EXCLUDED_SCHEMAS);
    }

    private static List<String> getTableIncludeExcludeListAsInValueList(String list) {
        return Strings.listOfTrimmed(list, s -> s.split("[,]"), v -> v.replaceAll("\\\\", ""));
    }

    private static List<String> toUpperCase(Collection<String> values) {
        return values.stream().map(String::toUpperCase).collect(Collectors.toList());
    }

    /**
     * Query helper to construct a SQL in-clause
     */
    private static class InClause {
        private String fieldName;
        private boolean negated;
        private boolean caseInsensitive;
        private Collection<?> values;

        private InClause() {
        }

        public static InClause builder() {
            return new InClause();
        }

        public InClause withField(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public InClause negate() {
            this.negated = true;
            return this;
        }

        public InClause caseInsensitive() {
            this.caseInsensitive = true;
            return this;
        }

        public InClause withValues(Collection<?> values) {
            this.values = values;
            return this;
        }

        public String build() {
            Objects.requireNonNull(fieldName, "The field name must not be null");
            Objects.requireNonNull(values, "The values list must not be null");

            final StringBuilder sql = new StringBuilder();
            if (caseInsensitive) {
                sql.append("UPPER(").append(fieldName).append(")");
            }
            else {
                sql.append(fieldName);
            }
            if (negated) {
                sql.append(" NOT");
            }
            sql.append(" IN (");
            sql.append(commaSeparatedList(values));
            sql.append(")");
            return sql.toString();
        }

        private String commaSeparatedList(Collection<?> values) {
            final StringBuilder list = new StringBuilder();
            for (Iterator<?> iterator = values.iterator(); iterator.hasNext();) {
                final Object value = iterator.next();
                if (value instanceof String) {
                    list.append("'").append(sanitizeCommaSeparatedStringElement((String) value)).append("'");
                }
                else {
                    list.append(value);
                }
                if (iterator.hasNext()) {
                    list.append(",");
                }
            }
            return list.toString();
        }

        private String sanitizeCommaSeparatedStringElement(String element) {
            if (caseInsensitive) {
                return element.trim().toUpperCase();
            }
            return element.trim();
        }
    }

    /**
     * Query helper to construct a SQL in-clause based a tuple of include/exclude value lists
     */
    private static class IncludeExcludeInClause {
        private String fieldName;
        private boolean caseInsensitive;
        private LogMiningQueryFilterMode mode;
        private List<Object> defaultIncludeValues;
        private List<Object> includeValues;
        private List<Object> defaultExcludeValues;
        private List<Object> excludeValues;

        private IncludeExcludeInClause() {
        }

        public static IncludeExcludeInClause builder() {
            return new IncludeExcludeInClause();
        }

        public IncludeExcludeInClause caseInsensitive() {
            this.caseInsensitive = true;
            return this;
        }

        public IncludeExcludeInClause withField(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public IncludeExcludeInClause withFilterMode(LogMiningQueryFilterMode mode) {
            this.mode = mode;
            return this;
        }

        public IncludeExcludeInClause withDefaultIncludeValues(Collection<?> values) {
            this.defaultIncludeValues = new ArrayList<>(values);
            return this;
        }

        public IncludeExcludeInClause withIncludeValues(Collection<?> values) {
            this.includeValues = new ArrayList<>(values);
            return this;
        }

        public IncludeExcludeInClause withDefaultExcludeValues(Collection<?> values) {
            this.defaultExcludeValues = new ArrayList<>(values);
            return this;
        }

        public IncludeExcludeInClause withExcludeValues(Collection<?> values) {
            this.excludeValues = new ArrayList<>(values);
            return this;
        }

        public String build() {
            Objects.requireNonNull(includeValues, "The include values must not be null");
            Objects.requireNonNull(excludeValues, "The exclude values must not be null");
            Objects.requireNonNull(mode, "The LogMiningQueryFilterMode must not be null");

            if (LogMiningQueryFilterMode.NONE.equals(mode) || (includeValues.isEmpty() && excludeValues.isEmpty())) {
                return EMPTY;
            }

            if (!LogMiningQueryFilterMode.IN.equals(mode)) {
                throw new IllegalStateException("Expected LogMiningQueryFilterMode to be IN");
            }

            final InClause inClause = InClause.builder().withField(fieldName);
            if (caseInsensitive) {
                inClause.caseInsensitive();
            }

            if (!excludeValues.isEmpty()) {
                if (defaultExcludeValues != null && !defaultExcludeValues.isEmpty()) {
                    defaultExcludeValues.addAll(excludeValues);
                    inClause.negate().withValues(defaultExcludeValues);
                }
                else {
                    inClause.negate().withValues(excludeValues);
                }
            }
            else {
                if (defaultIncludeValues != null && !defaultIncludeValues.isEmpty()) {
                    defaultIncludeValues.addAll(includeValues);
                    inClause.withValues(defaultIncludeValues);
                }
                else {
                    inClause.withValues(includeValues);
                }
            }

            return inClause.build();
        }
    }

    /**
     * Query helper to construct an Oracle SQL REGEXP_LIKE list based on include/exclude value lists
     */
    private static class IncludeExcludeRegExpLike {
        private String fieldName;
        private LogMiningQueryFilterMode mode;
        private List<Pattern> includeValues;
        private List<Pattern> excludeValues;

        private IncludeExcludeRegExpLike() {
        }

        public static IncludeExcludeRegExpLike builder() {
            return new IncludeExcludeRegExpLike();
        }

        public IncludeExcludeRegExpLike withField(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public IncludeExcludeRegExpLike withFilterMode(LogMiningQueryFilterMode mode) {
            this.mode = mode;
            return this;
        }

        public IncludeExcludeRegExpLike withIncludeValues(Collection<Pattern> values) {
            this.includeValues = new ArrayList<>(values);
            return this;
        }

        public IncludeExcludeRegExpLike withExcludeValues(Collection<Pattern> values) {
            this.excludeValues = new ArrayList<>(values);
            return this;
        }

        public String build() {
            Objects.requireNonNull(includeValues, "The include values must not be null");
            Objects.requireNonNull(excludeValues, "The exclude values must not be null");
            Objects.requireNonNull(mode, "The LogMiningQueryFilterMode must not be null");

            if (LogMiningQueryFilterMode.NONE.equals(mode) || (includeValues.isEmpty() && excludeValues.isEmpty())) {
                return EMPTY;
            }

            if (!LogMiningQueryFilterMode.REGEX.equals(mode)) {
                throw new IllegalStateException("Expected LogMiningQueryFilterMode to be REGEX");
            }

            final List<Pattern> values = !excludeValues.isEmpty() ? excludeValues : includeValues;
            final String prepend = !excludeValues.isEmpty() ? "NOT " : "";
            final String junction = !excludeValues.isEmpty() ? " AND " : " OR ";

            final StringBuilder sql = new StringBuilder();
            sql.append("(");
            for (Iterator<Pattern> iterator = values.iterator(); iterator.hasNext();) {
                sql.append(prepend);
                sql.append(regularExpressionLike(iterator.next()));
                if (iterator.hasNext()) {
                    sql.append(junction);
                }
            }
            sql.append(")");
            return sql.toString();
        }

        private String regularExpressionLike(Pattern pattern) {
            return "REGEXP_LIKE(" + fieldName + ",'" + preparePattern(pattern) + "','i')";
        }

        private String preparePattern(Pattern pattern) {
            // Connector configuration include/exclude lists are meant to match with an implied "^" and "$"
            // regular expression qualifiers. If the provided pattern does not explicitly include these,
            // this method will add those so that Oracle's REGEXP_LIKE does not match sub-text
            String patternText = pattern.pattern();
            if (!patternText.startsWith("^")) {
                patternText = "^" + patternText;
            }
            if (!patternText.endsWith("$")) {
                patternText += "$";
            }
            return patternText;
        }
    }
}
