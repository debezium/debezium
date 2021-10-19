/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOB_ENABLED;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST;
import static io.debezium.relational.history.DatabaseHistory.STORE_ONLY_CAPTURED_TABLES_DDL;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Unit test for the {@link LogMinerQueryBuilder}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerQueryBuilderTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static final String OPERATION_CODES_LOB_ENABLED = "(1,2,3,9,10,11,29)";
    private static final String OPERATION_CODES_LOB_DISABLED = "(1,2,3)";

    /**
     * A template that defines the expected SQL output when the configuration specifies
     * {@code database.history.store.only.captured.tables.ddl} is {@code false}.
     */
    private static final String LOG_MINER_CONTENT_QUERY_TEMPLATE1 = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, " +
            "XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK, RS_ID " +
            "FROM V$LOGMNR_CONTENTS WHERE SCN > ? AND SCN <= ? " +
            "AND SRC_CON_NAME = '" + TestHelper.DATABASE + "' " +
            "AND ((" +
            "OPERATION_CODE IN (6,7,34,36) OR " +
            "(OPERATION_CODE = 5 AND USERNAME NOT IN ('SYS','SYSTEM') " +
            "AND INFO NOT LIKE 'INTERNAL DDL%' " +
            "AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%')) ) " +
            "OR (OPERATION_CODE IN ${operationCodes} " +
            "AND TABLE_NAME != '" + LogWriterFlushStrategy.LOGMNR_FLUSH_TABLE + "' " +
            "${systemTablePredicate}" +
            "${schemaPredicate}" +
            "${tablePredicate}" +
            "))" +
            "${userNamePredicate}";

    /**
     * A template that defines the expected SQL output when the configuration specifies
     * {@code database.history.store.only.captured.tables.ddl} is {@code true}.
     */
    private static final String LOG_MINER_CONTENT_QUERY_TEMPLATE2 = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, " +
            "XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK, RS_ID " +
            "FROM V$LOGMNR_CONTENTS WHERE SCN > ? AND SCN <= ? " +
            "AND SRC_CON_NAME = '" + TestHelper.DATABASE + "' " +
            "AND ((" +
            "OPERATION_CODE IN (6,7,34,36)) OR " +
            "((OPERATION_CODE IN ${operationCodes} OR " +
            "(OPERATION_CODE = 5 AND USERNAME NOT IN ('SYS','SYSTEM') " +
            "AND INFO NOT LIKE 'INTERNAL DDL%' " +
            "AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%'))) " +
            "AND TABLE_NAME != '" + LogWriterFlushStrategy.LOGMNR_FLUSH_TABLE + "' " +
            "${systemTablePredicate}" +
            "${schemaPredicate}" +
            "${tablePredicate}" +
            "))" +
            "${userNamePredicate}";

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithNoFilters() {
        Configuration config = TestHelper.defaultConfig().build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        OracleDatabaseSchema schema = createSchema(connectorConfig);

        String result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, null, null, null));

        config = TestHelper.defaultConfig().with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, null, null, null));

        config = TestHelper.defaultConfig().with(STORE_ONLY_CAPTURED_TABLES_DDL, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, null, null, null));

        config = TestHelper.defaultConfig().with(STORE_ONLY_CAPTURED_TABLES_DDL, true).with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, null, null, null));
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaInclude() {
        String schema = "AND (REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') OR REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        assertQueryWithConfig(SCHEMA_INCLUDE_LIST, "SCHEMA1,SCHEMA2", schema, null, null);
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaExclude() {
        String schema = "AND (NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') AND NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        assertQueryWithConfig(OracleConnectorConfig.SCHEMA_EXCLUDE_LIST, "SCHEMA1,SCHEMA2", schema, null, null);
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithTableInclude() {
        String table = "AND (REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "OR REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";
        assertQueryWithConfig(TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB", null, table, null);
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithTableExcludes() {
        String table = "AND (NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "AND NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";
        assertQueryWithConfig(TABLE_EXCLUDE_LIST, "DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB", null, table, null);
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaTableIncludes() {
        String schema = "AND (REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') OR REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        String table = "AND (REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "OR REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";
        assertQueryWithConfig(SCHEMA_INCLUDE_LIST, "SCHEMA1,SCHEMA2", TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB", schema, table);
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaTableExcludes() {
        String schema = "AND (NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') AND NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        String table = "AND (NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "AND NOT REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";
        assertQueryWithConfig(SCHEMA_EXCLUDE_LIST, "SCHEMA1,SCHEMA2", TABLE_EXCLUDE_LIST, "DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB", schema, table);
    }

    @Test
    @FixFor("DBZ-3009")
    public void testLogMinerQueryWithSchemaExcludeTableInclude() {
        String schema = "AND (NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA1$','i') AND NOT REGEXP_LIKE(SEG_OWNER,'^SCHEMA2$','i')) ";
        String table = "AND (REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEA$','i') " +
                "OR REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^DEBEZIUM\\.TABLEB$','i')) ";
        assertQueryWithConfig(SCHEMA_EXCLUDE_LIST, "SCHEMA1,SCHEMA2", TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLEA,DEBEZIUM\\.TABLEB", schema, table);
    }

    @Test
    @FixFor("DBZ-3671")
    public void testLogMinerExcludeUsersInQuery() {
        String users = " AND USERNAME NOT IN ('user1','user2','user')";
        assertQueryWithConfig(OracleConnectorConfig.LOG_MINING_USERNAME_EXCLUDE_LIST, "user1,user2,user", null, null, users);
    }

    private void assertQueryWithConfig(Field field, Object fieldValue, String schemaValue, String tableValue, String userValue) {
        Configuration config = TestHelper.defaultConfig().with(field, fieldValue).build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        OracleDatabaseSchema schema = createSchema(connectorConfig);

        String result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, userValue));

        config = TestHelper.defaultConfig().with(field, fieldValue).with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, userValue));

        config = TestHelper.defaultConfig().with(field, fieldValue).with(STORE_ONLY_CAPTURED_TABLES_DDL, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, userValue));

        config = TestHelper.defaultConfig().with(field, fieldValue).with(STORE_ONLY_CAPTURED_TABLES_DDL, true).with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, userValue));
    }

    private void assertQueryWithConfig(Field field1, Object fieldValue1, Field field2, Object fieldValue2, String schemaValue, String tableValue) {
        Configuration config = TestHelper.defaultConfig().with(field1, fieldValue1).with(field2, fieldValue2).build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        OracleDatabaseSchema schema = createSchema(connectorConfig);

        String result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, null));

        config = TestHelper.defaultConfig().with(field1, fieldValue1).with(field2, fieldValue2).with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, null));

        config = TestHelper.defaultConfig().with(field1, fieldValue1).with(field2, fieldValue2)
                .with(STORE_ONLY_CAPTURED_TABLES_DDL, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, null));

        config = TestHelper.defaultConfig().with(field1, fieldValue1).with(field2, fieldValue2)
                .with(STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(LOB_ENABLED, true)
                .build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig, schema);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig, schema, schemaValue, tableValue, null));
    }

    private String resolveLogMineryContentQueryFromTemplate(OracleConnectorConfig config,
                                                            OracleDatabaseSchema schema,
                                                            String schemaReplacement,
                                                            String tableReplacement,
                                                            String userNameReplacement) {
        String query = schema.storeOnlyCapturedTables()
                ? LOG_MINER_CONTENT_QUERY_TEMPLATE2
                : LOG_MINER_CONTENT_QUERY_TEMPLATE1;

        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            StringBuilder systemPredicate = new StringBuilder();
            systemPredicate.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                systemPredicate.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    systemPredicate.append(",");
                }
            }
            systemPredicate.append(") ");
            query = query.replace("${systemTablePredicate}", systemPredicate.toString());
        }
        else {
            query = query.replace("${systemTablePredicate}", "");
        }

        query = query.replace("${operationCodes}", config.isLobEnabled() ? OPERATION_CODES_LOB_ENABLED : OPERATION_CODES_LOB_DISABLED);
        query = query.replace("${schemaPredicate}", schemaReplacement == null ? "" : schemaReplacement);
        query = query.replace("${tablePredicate}", tableReplacement == null ? "" : tableReplacement);
        query = query.replace("${userNamePredicate}", userNameReplacement == null ? "" : userNameReplacement.toString());
        return query;
    }

    private OracleDatabaseSchema createSchema(OracleConnectorConfig connectorConfig) {
        OracleConnection connection = Mockito.mock(OracleConnection.class);
        OracleValueConverters converters = new OracleValueConverters(connectorConfig, connection);
        TableNameCaseSensitivity tableNameSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(connection);

        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        return new OracleDatabaseSchema(connectorConfig, converters, schemaNameAdjuster, topicSelector, tableNameSensitivity);
    }
}
