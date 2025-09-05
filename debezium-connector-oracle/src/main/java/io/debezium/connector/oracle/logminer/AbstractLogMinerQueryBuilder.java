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
 * An abstract base implementation of {@link LogMinerQueryBuilder}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerQueryBuilder implements LogMinerQueryBuilder {

    private static final String EMPTY = "";
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";
    private static final String UNKNOWN_USERNAME = "UNKNOWN";
    private static final String UNKNOWN_SCHEMA_NAME = "UNKNOWN";
    private static final String UNKNOWN_TABLE_NAME_PREFIX = "OBJ#";

    public static final Integer IN_CLAUSE_MAX_ELEMENTS = 1000;

    protected final OracleConnectorConfig connectorConfig;
    protected final boolean useCteQuery;

    public AbstractLogMinerQueryBuilder(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.useCteQuery = connectorConfig.isLogMiningUseCteQuery();
    }

    @Override
    public String getQuery() {
        final String whereClause = getPredicates(false);

        final StringBuilder query = new StringBuilder(1024);
        if (useCteQuery) {
            final String cteWhereClause = getPredicates(true);
            query.append("WITH relevant_xids AS (")
                    .append("SELECT DISTINCT XID as RXID FROM V$LOGMNR_CONTENTS ")
                    .append(!Strings.isNullOrBlank(cteWhereClause) ? "WHERE " + cteWhereClause : "")
                    .append(") ");
        }

        query.append("SELECT ");

        if (useCteQuery) {
            // Preserves the join hash order based on LOGMNR_CONTENTS_VIEW and not the CTE
            query.append("/*+ ORDERED USE_NL(R) */ ");
        }

        query.append("SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, ")
                .append("USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD#, DATA_OBJ#, DATA_OBJV#, DATA_OBJD#, ")
                .append("CLIENT_ID, START_SCN, COMMIT_SCN, START_TIMESTAMP, COMMIT_TIMESTAMP, SEQUENCE# ")
                .append("FROM ")
                .append(LOGMNR_CONTENTS_VIEW).append(" ");

        if (useCteQuery) {
            query.append("V JOIN relevant_xids R ON R.RXID = V.XID ");
        }

        return query.append(!Strings.isNullOrEmpty(whereClause) ? "WHERE " + whereClause : "").toString();
    }

    /**
     * Provides a way for various implementations to create their query predicate clauses.
     *
     * @param isCteQuery whether the predicates should be built for the CTE query
     * @return a series of predicates.
     */
    protected abstract String getPredicates(boolean isCteQuery);

    /**
     * Get the multi-tenant predicate based on the configured pluggable database name.
     *
     * @return multi-tenant predicate, may be an empty string if multi-tenancy is not enabled
     */
    protected String getMultiTenantPredicate() {
        if (!Strings.isNullOrEmpty(connectorConfig.getPdbName())) {
            return "SRC_CON_NAME = '" + connectorConfig.getPdbName() + "'";
        }
        return EMPTY;
    }

    /**
     * Generate a username based predicate for include/exclude usernames.
     *
     * @return the username predicate, may be an empty string if no predicate is generated
     */
    protected String getUserNamePredicate() {
        if (!LogMiningQueryFilterMode.NONE.equals(connectorConfig.getLogMiningQueryFilterMode())) {
            // Only filter usernames when using IN and REGEX modes
            // Username filters always use an IN-clause predicate
            //
            // Oracle always stores usernames as upper-case.
            // This predicate build applies upper-case to the provided value lists from the configuration.
            return applyToNonTransactionMarkers(IncludeExcludeInClause.builder()
                    .withField("USERNAME")
                    .withFilterMode(LogMiningQueryFilterMode.IN)
                    .withDefaultIncludeValues(Collections.singletonList(UNKNOWN_USERNAME))
                    .withIncludeValues(connectorConfig.getLogMiningUsernameIncludes())
                    .withExcludeValues(connectorConfig.getLogMiningUsernameExcludes())
                    .caseInsensitive()
                    .build());
        }
        return EMPTY;
    }

    /**
     * Generate a client id based predicate for include/excluded client identifiers.
     *
     * @return the client id predicate, will be an empty string if no predicate is generated, never {@code null}
     */
    protected String getClientIdPredicate() {
        if (!LogMiningQueryFilterMode.NONE.equals(connectorConfig.getLogMiningQueryFilterMode())) {
            // Only filter client ids when using IN and REGEX modes
            // Client id filters always use an IN-clause predicate
            return applyToNonTransactionMarkers(IncludeExcludeInClause.builder()
                    .withField("CLIENT_ID")
                    .withFilterMode(LogMiningQueryFilterMode.IN)
                    .withIncludeValues(connectorConfig.getLogMiningClientIdIncludes())
                    .withExcludeValues(connectorConfig.getLogMiningClientIdExcludes())
                    .caseInsensitive()
                    .build());
        }
        return EMPTY;
    }

    private String applyToNonTransactionMarkers(String fragment) {
        if (!Strings.isNullOrBlank(fragment)) {
            return "(CASE WHEN OPERATION_CODE IN (6,7,36) THEN 1 ELSE CASE WHEN " + fragment + " THEN 1 ELSE 0 END END = 1)";
        }
        return fragment;
    }

    /**
     * Generates a predicate based on the table include/exclude lists, if database filtering is enabled.
     *
     * @return the table include/exclude predicate or an empty string if no predicate is generated, never {@code null}
     */
    protected String getTableNamePredicate() {
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
            if (connectorConfig.getLogMiningStrategy() == OracleConnectorConfig.LogMiningStrategy.HYBRID) {
                predicate.append("TABLE_NAME LIKE '").append(UNKNOWN_TABLE_NAME_PREFIX).append("%' OR ");
            }

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
            if (connectorConfig.getLogMiningStrategy() == OracleConnectorConfig.LogMiningStrategy.HYBRID) {
                predicate.append("TABLE_NAME LIKE '").append(UNKNOWN_TABLE_NAME_PREFIX).append("%' OR ");
            }

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

    /**
     * Get the schema-based predicate.
     *
     * @return the schema predicate, may be an empty string if no predicate is generated
     */
    protected String getSchemaNamePredicate() {
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
    protected static class InClause {
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
            final List<?> listValues = Arrays.asList(values.toArray());
            final int buckets = (listValues.size() + IN_CLAUSE_MAX_ELEMENTS - 1) / IN_CLAUSE_MAX_ELEMENTS;

            for (int i = 0; i < buckets; i++) {
                if (i > 0) {
                    sql.append(negated ? " AND " : " OR ");
                }
                if (caseInsensitive) {
                    sql.append("UPPER(").append(fieldName).append(")");
                }
                else {
                    sql.append(fieldName);
                }
                if (negated) {
                    sql.append(" NOT");
                }

                final int startIndex = (i * IN_CLAUSE_MAX_ELEMENTS);
                final int endIndex = startIndex + Math.min(IN_CLAUSE_MAX_ELEMENTS, listValues.size() - startIndex);
                sql.append(" IN (").append(commaSeparatedList(listValues.subList(startIndex, endIndex))).append(")");
            }

            return (listValues.size() > IN_CLAUSE_MAX_ELEMENTS) ? "(" + sql + ")" : sql.toString();
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
