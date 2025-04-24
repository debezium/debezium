/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.config.CommonConnectorConfig.SIGNAL_DATA_COLLECTION;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOB_ENABLED;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_TYPE;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_USERNAME_EXCLUDE_LIST;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_USERNAME_INCLUDE_LIST;
import static io.debezium.connector.oracle.OracleConnectorConfig.PDB_NAME;
import static io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerQueryBuilder.IN_CLAUSE_MAX_ELEMENTS;
import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningQueryFilterMode;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerQueryBuilder;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * Unit test for the {@link BufferedLogMinerQueryBuilder}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerQueryBuilderTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static final String LOG_MINER_QUERY_BASE = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, " +
            "XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, " +
            "THREAD#, DATA_OBJ#, DATA_OBJV#, DATA_OBJD#, CLIENT_ID FROM V$LOGMNR_CONTENTS " +
            "WHERE SCN > ? AND SCN <= ?";

    private static final String PDB_PREDICATE = "SRC_CON_NAME = '${pdbName}'";

    private static final String OPERATION_CODES_LOB_ENABLED = "1,2,3,6,7,9,10,11,27,29,34,36,68,70,71,91,92,93,255";
    private static final String OPERATION_CODES_LOB_DISABLED = "1,2,3,7,27,34,36,255";
    private static final String OPERATION_CODES_LOB_DISABLED_AND_PERSISTENT_BUFFER = "1,2,3,6,7,27,34,36,255";

    private static final String OPERATION_CODES_PREDICATE = "(OPERATION_CODE IN (${operationCodes})${operationDdl})";

    @Test
    public void testLogMinerQueryFilterNone() {
        testLogMinerQueryFilterMode(LogMiningQueryFilterMode.NONE);
    }

    @Test
    public void testLogMinerQueryFilterIn() {
        testLogMinerQueryFilterMode(LogMiningQueryFilterMode.IN);
    }

    @Test
    public void testLogMinerQueryFilterRegEx() {
        testLogMinerQueryFilterMode(LogMiningQueryFilterMode.REGEX);
    }

    @Test
    @FixFor("DBZ-5648")
    public void testLogMinerQueryWithLobDisabled() {
        Configuration config = TestHelper.defaultConfig().build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);

        String result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));

        config = TestHelper.defaultConfig().with(PDB_NAME, "").build();
        connectorConfig = new OracleConnectorConfig(config);

        result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));
    }

    @Test
    @FixFor("DBZ-7473")
    public void testLogMinerQueryWithLobDisabledAndPersistentBuffer() {
        Configuration config = TestHelper.defaultConfig()
                .with(LOG_MINING_BUFFER_TYPE, OracleConnectorConfig.LogMiningBufferType.INFINISPAN_EMBEDDED)
                .build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);

        String result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));

        config = TestHelper.defaultConfig().with(PDB_NAME, "").build();
        connectorConfig = new OracleConnectorConfig(config);

        result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));
    }

    @Test
    @FixFor("DBZ-5648")
    public void testLogMinerQueryWithLobEnabled() {
        Configuration config = TestHelper.defaultConfig().with(LOB_ENABLED, true).build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);

        String result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));

        config = TestHelper.defaultConfig().with(PDB_NAME, "").with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));
    }

    @Test
    @FixFor("DBZ-7847")
    public void testTableIncludeListWithMoreThan1000Elements() {
        StringBuilder tables = new StringBuilder();
        for (int i = 0; i < 1001; i++) {
            if (i > 0) {
                tables.append(",");
            }
            tables.append("DEBEZIUM\\.T" + i);
        }
        assertQuery(getBuilderForMode(LogMiningQueryFilterMode.IN).with(TABLE_INCLUDE_LIST, tables.toString()));
        assertQuery(getBuilderForMode(LogMiningQueryFilterMode.IN).with(TABLE_EXCLUDE_LIST, tables.toString()));

        tables = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            if (i > 0) {
                tables.append(",");
            }
            tables.append("DEBEZIUM\\.T" + i);
        }
        assertQuery(getBuilderForMode(LogMiningQueryFilterMode.IN).with(TABLE_INCLUDE_LIST, tables.toString()));
        assertQuery(getBuilderForMode(LogMiningQueryFilterMode.IN).with(TABLE_EXCLUDE_LIST, tables.toString()));
    }

    @Test
    @FixFor("DBZ-8904")
    public void testClientIdIncludeExclude() {
        Configuration config = TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_CLIENTID_EXCLUDE_LIST, "abc,xyz").build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);

        String result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));

        config = TestHelper.defaultConfig().with(OracleConnectorConfig.LOG_MINING_CLIENTID_INCLUDE_LIST, "abc,xyz").build();
        connectorConfig = new OracleConnectorConfig(config);

        result = BufferedLogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(getQueryFromTemplate(connectorConfig));
    }

    private void testLogMinerQueryFilterMode(LogMiningQueryFilterMode mode) {
        // Default configuration
        assertQuery(getBuilderForMode(mode));

        // Schema Includes/Excludes
        final String schemas = "DEBEZIUM1,DEBEZIUM2, DEBEZIUM3, DEBEZIUM4";
        assertQuery(getBuilderForMode(mode).with(SCHEMA_INCLUDE_LIST, schemas));
        assertQuery(getBuilderForMode(mode).with(SCHEMA_EXCLUDE_LIST, schemas));

        // Table Include/Excludes
        final String tables = "DEBEZIUM\\.T1,DEBEZIUM\\.T2, DEBEZIUM\\.T3, DEBEZIUM\\.T4";
        assertQuery(getBuilderForMode(mode).with(TABLE_INCLUDE_LIST, tables));
        assertQuery(getBuilderForMode(mode).with(SCHEMA_EXCLUDE_LIST, tables));

        // Username Include/Excludes
        final String users = "U1,U2, U3, U4";
        assertQuery(getBuilderForMode(mode).with(LOG_MINING_USERNAME_INCLUDE_LIST, users));
        assertQuery(getBuilderForMode(mode).with(LOG_MINING_USERNAME_EXCLUDE_LIST, users));

        // Table Includes/Exclude without Signal Collection Table + Signal Data Collection Specified
        final String signalTable = TestHelper.getDatabaseName() + ".DEBEZIUM1.SIGNAL_TABLE";
        assertQuery(getBuilderForMode(mode).with(TABLE_INCLUDE_LIST, tables).with(SIGNAL_DATA_COLLECTION, signalTable));
        assertQuery(getBuilderForMode(mode).with(TABLE_EXCLUDE_LIST, tables).with(SIGNAL_DATA_COLLECTION, signalTable));

        // Table Include with Signal Collection Table + Signal Data Collection Specified
        final String tables2 = tables + ",DEBEZIUM1\\.SIGNAL_TABLE";
        assertQuery(getBuilderForMode(mode).with(TABLE_INCLUDE_LIST, tables2).with(SIGNAL_DATA_COLLECTION, signalTable));

        // Complex Multi-Include/Multi-Exclude scenario
        assertQuery(getBuilderForMode(mode).with(SCHEMA_INCLUDE_LIST, schemas).with(TABLE_INCLUDE_LIST, tables).with(LOG_MINING_USERNAME_INCLUDE_LIST, users));
        assertQuery(getBuilderForMode(mode).with(SCHEMA_EXCLUDE_LIST, schemas).with(TABLE_EXCLUDE_LIST, tables).with(LOG_MINING_USERNAME_EXCLUDE_LIST, users));

        // Complex Mash-up Include/Excludes
        assertQuery(getBuilderForMode(mode).with(SCHEMA_INCLUDE_LIST, schemas).with(TABLE_EXCLUDE_LIST, tables).with(LOG_MINING_USERNAME_INCLUDE_LIST, users));
    }

    private ConfigBuilder getBuilderForMode(LogMiningQueryFilterMode mode) {
        return new ConfigBuilder().with(LOG_MINING_QUERY_FILTER_MODE, mode.getValue());
    }

    private void assertQuery(ConfigBuilder builder) {
        // STORE_ONLY_CAPTURED_TABLES_DDL default (false)
        OracleConnectorConfig config = builder.with(STORE_ONLY_CAPTURED_TABLES_DDL, "false").build();
        assertThat(BufferedLogMinerQueryBuilder.build(config)).isEqualTo(getQueryFromTemplate(config));

        // STORE_ONLY_CAPTURED_TABLES_DDL non-default (true)
        config = builder.with(STORE_ONLY_CAPTURED_TABLES_DDL, "true").build();
        assertThat(BufferedLogMinerQueryBuilder.build(config)).isEqualTo(getQueryFromTemplate(config));
    }

    private String getQueryFromTemplate(OracleConnectorConfig config) {
        String query = LOG_MINER_QUERY_BASE;
        query += getPdbPredicate(config);
        query += " AND ";

        if (!config.storeOnlyCapturedTables()) {
            query += "((";
        }

        query += getOperationCodePredicate(config);
        query += getUserNamePredicate(config);
        query += getClientIdPredicate(config);
        query += getSchemaNamesPredicate(config);
        query += getTableNamesPredicate(config);

        if (!config.storeOnlyCapturedTables()) {
            query += ")" + getOperationDdlPredicate() + ")";
        }

        return query;
    }

    private String getPdbPredicate(OracleConnectorConfig config) {
        if (!Strings.isNullOrEmpty(config.getPdbName())) {
            return " AND " + PDB_PREDICATE.replace("${pdbName}", config.getPdbName());
        }
        return "";
    }

    private String getOperationCodePredicate(OracleConnectorConfig config) {
        final String codes = config.isLobEnabled() ? OPERATION_CODES_LOB_ENABLED
                : (config.getLogMiningBufferType() == OracleConnectorConfig.LogMiningBufferType.MEMORY)
                        ? OPERATION_CODES_LOB_DISABLED
                        : OPERATION_CODES_LOB_DISABLED_AND_PERSISTENT_BUFFER;
        final String predicate = OPERATION_CODES_PREDICATE.replace("${operationCodes}", codes);
        return predicate.replace("${operationDdl}", config.storeOnlyCapturedTables() ? getOperationDdlPredicate() : "");
    }

    private String getOperationDdlPredicate() {
        return " OR (OPERATION_CODE = 5 AND INFO NOT LIKE 'INTERNAL DDL%')";
    }

    private String getUserNamePredicate(OracleConnectorConfig config) {
        final LogMiningQueryFilterMode queryFilterMode = config.getLogMiningQueryFilterMode();
        final Set<String> includes = config.getLogMiningUsernameIncludes();
        final Set<String> excludes = config.getLogMiningUsernameExcludes();

        if (!includes.isEmpty() && !queryFilterMode.equals(LogMiningQueryFilterMode.NONE)) {
            return " AND UPPER(USERNAME) IN ('UNKNOWN'," + includes.stream().map(this::quote).collect(Collectors.joining(",")) + ")";
        }
        else if (!excludes.isEmpty() && !queryFilterMode.equals(LogMiningQueryFilterMode.NONE)) {
            return " AND UPPER(USERNAME) NOT IN (" + excludes.stream().map(this::quote).collect(Collectors.joining(",")) + ")";
        }
        else {
            return "";
        }
    }

    private String getClientIdPredicate(OracleConnectorConfig config) {
        final LogMiningQueryFilterMode queryFilterMode = config.getLogMiningQueryFilterMode();
        final Set<String> includes = config.getLogMiningClientIdIncludes();
        final Set<String> excludes = config.getLogMiningClientIdExcludes();

        if (!includes.isEmpty() && !queryFilterMode.equals(LogMiningQueryFilterMode.NONE)) {
            return " AND UPPER(CLIENT_ID) IN (" + includes.stream().map(this::quote).collect(Collectors.joining(",")) + ")";
        }
        else if (!excludes.isEmpty() && !queryFilterMode.equals(LogMiningQueryFilterMode.NONE)) {
            return " AND UPPER(CLIENT_ID) NOT IN (" + excludes.stream().map(this::quote).collect(Collectors.joining(",")) + ")";
        }
        else {
            return "";
        }
    }

    private Set<String> getExcludedSchemas() {
        return OracleConnectorConfig.EXCLUDED_SCHEMAS.stream().map(String::toUpperCase).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private String getSchemaNamesPredicate(OracleConnectorConfig config) {
        final String fieldName = "SEG_OWNER";
        final String includeList = config.schemaIncludeList();
        final String excludeList = config.schemaExcludeList();
        if (LogMiningQueryFilterMode.NONE.equals(config.getLogMiningQueryFilterMode()) ||
                (Strings.isNullOrEmpty(includeList) && Strings.isNullOrEmpty(excludeList))) {
            // Only built-in excluded schemas within in-clause
            return " AND (" + fieldName + " IS NULL OR " + getIn(fieldName, getExcludedSchemas(), true, false) + ")";
        }
        else if (config.getLogMiningQueryFilterMode().equals(LogMiningQueryFilterMode.IN)) {
            // Use IN-clauses
            final String inClause;
            if (!Strings.isNullOrEmpty(includeList)) {
                inClause = getIn(fieldName, getSchemaIncludes(includeList, false), false, true);
            }
            else {
                // Exclusions are defined (either built-in or both built-in and provided
                inClause = getIn(fieldName, getSchemaExcludes(excludeList, false), true, true);
            }
            return " AND (" + fieldName + " IS NULL OR " + inClause + ")";
        }
        else {
            // Regular Expressions
            final String otherClause;
            final String regExpLikeClause;
            if (!Strings.isNullOrEmpty(includeList)) {
                // Inclusions are defined
                regExpLikeClause = getRegexpLike(fieldName, getSchemaIncludes(includeList, true), false);
                otherClause = fieldName + " = 'UNKNOWN'";
            }
            else {
                // Exclusions are defined (either built-in or both built-in and provided
                regExpLikeClause = getRegexpLike(fieldName, getSchemaExcludes(excludeList, true), true);
                otherClause = fieldName + " NOT IN (" + getExcludedSchemas().stream().map(v -> "'" + v + "'").collect(Collectors.joining(",")) + ")";
            }
            return " AND (" + fieldName + " IS NULL OR " + otherClause + " OR " + regExpLikeClause + ")";
        }
    }

    private String getTableNamesPredicate(OracleConnectorConfig config) {
        final String fieldName = "SEG_OWNER || '.' || TABLE_NAME";
        final String includeList = config.tableIncludeList();
        final String excludeList = config.tableExcludeList();
        if (config.getLogMiningQueryFilterMode().equals(LogMiningQueryFilterMode.NONE) ||
                (Strings.isNullOrEmpty(includeList) && Strings.isNullOrEmpty(excludeList))) {
            return "";
        }
        else if (config.getLogMiningQueryFilterMode().equals(LogMiningQueryFilterMode.IN)) {
            // Use IN-clauses
            final String inClause;
            if (!Strings.isNullOrEmpty(includeList)) {
                inClause = getIn(fieldName, getTableIncludeOrExclude(includeList, false), false, true);
            }
            else {
                inClause = getIn(fieldName, getTableIncludeOrExclude(excludeList, false), true, true);
            }
            final String signalDataClause = getSignalDataCollectionTableClause(config);
            if (config.getLogMiningStrategy() == OracleConnectorConfig.LogMiningStrategy.HYBRID) {
                return " AND (TABLE_NAME IS NULL OR TABLE_NAME LIKE 'OBJ#%' OR " + signalDataClause + inClause + ")";
            }
            return " AND (TABLE_NAME IS NULL OR " + signalDataClause + inClause + ")";
        }
        else {
            // Regular Expressions
            final String regExpLikeClause;
            if (!Strings.isNullOrEmpty(includeList)) {
                regExpLikeClause = getRegexpLike(fieldName, getTableIncludeOrExclude(includeList, true), false);
            }
            else {
                regExpLikeClause = getRegexpLike(fieldName, getTableIncludeOrExclude(excludeList, true), true);
            }
            final String signalDataClause = getSignalDataCollectionTableClause(config);
            if (config.getLogMiningStrategy() == OracleConnectorConfig.LogMiningStrategy.HYBRID) {
                return " AND (TABLE_NAME IS NULL OR TABLE_NAME LIKE 'OBJ#%' OR " + signalDataClause + regExpLikeClause + ")";
            }
            return " AND (TABLE_NAME IS NULL OR " + signalDataClause + regExpLikeClause + ")";
        }
    }

    private String getSignalDataCollectionTableClause(OracleConnectorConfig config) {
        if (!Strings.isNullOrEmpty(config.getSignalingDataCollectionId())) {
            final TableId tableId = TableId.parse(config.getSignalingDataCollectionId());

            boolean foundMatch = false;
            final List<Pattern> includeList = Strings.listOfRegex(config.tableIncludeList(), Pattern.CASE_INSENSITIVE);
            for (Pattern pattern : includeList) {
                if (config.getLogMiningQueryFilterMode().equals(LogMiningQueryFilterMode.REGEX)) {
                    if (pattern.matcher(tableId.identifier()).matches()) {
                        foundMatch = true;
                        break;
                    }
                }
                else {
                    if (pattern.matcher(tableId.schema() + "." + tableId.table()).matches()) {
                        foundMatch = true;
                        break;
                    }
                }
            }

            if (!foundMatch) {
                // User did not include the signal table in the include list
                // We need to explicitly add it.
                return new StringBuilder()
                        .append("UPPER(SEG_OWNER || '.' || TABLE_NAME) = '")
                        .append(tableId.schema().toUpperCase())
                        .append('.')
                        .append(tableId.table().toUpperCase())
                        .append("' OR ")
                        .toString();
            }
        }
        return "";
    }

    private String getIn(String columnName, Collection<String> values, boolean negated, boolean caseInsensitive) {
        final StringBuilder predicate = new StringBuilder();

        final List<?> listValues = Arrays.asList(values.toArray());
        final int buckets = (listValues.size() + IN_CLAUSE_MAX_ELEMENTS - 1) / IN_CLAUSE_MAX_ELEMENTS;
        for (int bucket = 0; bucket < buckets; bucket++) {
            if (bucket > 0) {
                predicate.append(negated ? " AND " : " OR ");
            }
            if (caseInsensitive) {
                predicate.append("UPPER(").append(columnName).append(")");
            }
            else {
                predicate.append(columnName);
            }
            if (negated) {
                predicate.append(" NOT");
            }
            predicate.append(" IN (");

            final int startIndex = (bucket * IN_CLAUSE_MAX_ELEMENTS);
            final int endIndex = startIndex + Math.min(IN_CLAUSE_MAX_ELEMENTS, listValues.size() - startIndex);
            final List<?> subList = listValues.subList(startIndex, endIndex);

            for (Iterator<?> iterator = subList.iterator(); iterator.hasNext();) {
                final Object value = iterator.next();
                predicate.append("'").append(value).append("'");
                if (iterator.hasNext()) {
                    predicate.append(",");
                }
            }

            predicate.append(")");
        }

        return listValues.size() > IN_CLAUSE_MAX_ELEMENTS ? "(" + predicate + ")" : predicate.toString();
    }

    private String getRegexpLike(String columnName, Collection<Pattern> values, boolean negated) {
        final StringBuilder predicate = new StringBuilder();
        predicate.append("(");
        for (Iterator<Pattern> iterator = values.iterator(); iterator.hasNext();) {
            if (negated) {
                predicate.append("NOT ");
            }
            final Pattern pattern = iterator.next();
            predicate.append("REGEXP_LIKE(");
            predicate.append(columnName).append(",");
            predicate.append("'^").append(pattern.pattern()).append("$','i')");
            if (iterator.hasNext()) {
                predicate.append(negated ? " AND " : " OR ");
            }
        }
        predicate.append(")");
        return predicate.toString();
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> getSchemaIncludes(String schemaIncludeList, boolean regex) {
        // When applying schema inclusions, we also want to specify the UNKNOWN variants
        final List<Object> inclusions = new ArrayList<>();
        if (!regex) {
            inclusions.add("UNKNOWN");
            inclusions.addAll(Strings.setOfTrimmed(schemaIncludeList, String::trim));
        }
        else {
            inclusions.addAll(Strings.listOfRegex(schemaIncludeList, Pattern.CASE_INSENSITIVE));
        }
        return (List<T>) inclusions;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> getSchemaExcludes(String schemaExcludeList, boolean regex) {
        // This is always a combination of the built-in excludes plus user defined excludes
        final List<Object> exclusions = new ArrayList<>();
        if (!regex) {
            exclusions.addAll(getExcludedSchemas());
            if (!Strings.isNullOrEmpty(schemaExcludeList)) {
                exclusions.addAll(Strings.setOfTrimmed(schemaExcludeList, String::new));
            }
        }
        else if (!Strings.isNullOrEmpty(schemaExcludeList)) {
            exclusions.addAll(Strings.listOfRegex(schemaExcludeList, Pattern.CASE_INSENSITIVE));
        }
        return (List<T>) exclusions;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> getTableIncludeOrExclude(String list, boolean regex) {
        final List<Object> values = new ArrayList<>();
        if (!regex) {
            // Explicitly replace all escaped characters due to Regex.
            values.addAll(Strings.listOfTrimmed(list, s -> s.split("[,]"), v -> v.replaceAll("\\\\", "")));
        }
        else {
            values.addAll(Strings.listOfRegex(list, Pattern.CASE_INSENSITIVE));
        }
        return (List<T>) values;
    }

    private String quote(String value) {
        return "'" + value + "'";
    }

    private class ConfigBuilder {
        private final Configuration.Builder builder = TestHelper.defaultConfig();

        public ConfigBuilder with(Field field, String value) {
            builder.with(field, value);
            return this;
        }

        public OracleConnectorConfig build() {
            return new OracleConnectorConfig(builder.build());
        }
    }

}
