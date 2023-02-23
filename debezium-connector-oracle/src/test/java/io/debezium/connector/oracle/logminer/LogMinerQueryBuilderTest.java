/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOB_ENABLED;
import static io.debezium.connector.oracle.OracleConnectorConfig.PDB_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.util.Strings;

/**
 * Unit test for the {@link LogMinerQueryBuilder}.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerQueryBuilderTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    private static final String OPERATION_CODES_LOB_ENABLED = "(1,2,3,5,6,7,9,10,11,29,34,36,255)";
    private static final String OPERATION_CODES_LOB_DISABLED = "(1,2,3,5,6,7,34,36,255)";
    private static final String OPERATION_CODES_PDB_LOB_ENABLED = "(1,2,3,5,9,10,11,29,34,255)";
    private static final String OPERATION_CODES_PDB_LOB_DISABLED = "(1,2,3,5,34,255)";

    private static final String LOG_MINER_CONTEXT_QUERY = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, " +
            "XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, " +
            "THREAD# FROM V$LOGMNR_CONTENTS " +
            "WHERE SCN > ? AND SCN <= ? " +
            "${systemTablePredicate}" +
            "${operationCodesTemplate}";

    private static final String OPERATION_CODES_PDB_TEMPLATE = "AND (OPERATION_CODE IN (6,7,36) " +
            "OR (${pdbPredicate} AND OPERATION_CODE IN ${operationCodes}))";

    private static final String OPERATION_CODES_NON_PDB_TEMPLATE = "AND OPERATION_CODE IN ${operationCodes}";

    @Test
    @FixFor("DBZ-5648")
    public void testLogMinerQueryWithLobDisabled() {
        Configuration config = TestHelper.defaultConfig().build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);

        String result = LogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig));

        config = TestHelper.defaultConfig().with(PDB_NAME, "").build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig));
    }

    @Test
    @FixFor("DBZ-5648")
    public void testLogMinerQueryWithLobEnabled() {
        Configuration config = TestHelper.defaultConfig().with(LOB_ENABLED, true).build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);

        String result = LogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig));

        config = TestHelper.defaultConfig().with(PDB_NAME, "").with(LOB_ENABLED, true).build();
        connectorConfig = new OracleConnectorConfig(config);

        result = LogMinerQueryBuilder.build(connectorConfig);
        assertThat(result).isEqualTo(resolveLogMineryContentQueryFromTemplate(connectorConfig));
    }

    private String resolveLogMineryContentQueryFromTemplate(OracleConnectorConfig config) {
        String query = LOG_MINER_CONTEXT_QUERY;

        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            StringBuilder systemPredicate = new StringBuilder();
            systemPredicate.append("AND (SEG_OWNER IS NULL ");
            systemPredicate.append("OR SEG_OWNER NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                systemPredicate.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    systemPredicate.append(",");
                }
            }
            systemPredicate.append(")) ");
            query = query.replace("${systemTablePredicate}", systemPredicate.toString());
        }
        else {
            query = query.replace("${systemTablePredicate}", "");
        }

        String template = getOperationCodesTemplate(config);
        template = template.replace("${pdbPredicate}", getPdbPredicate(config));
        template = template.replace("${operationCodes}", getOperationCodes(config));
        query = query.replace("${operationCodesTemplate}", template);

        System.out.println(query);
        return query;
    }

    private String getPdbPredicate(OracleConnectorConfig config) {
        if (!Strings.isNullOrBlank(config.getPdbName())) {
            return "SRC_CON_NAME = '" + TestHelper.DATABASE + "'";
        }
        return "";
    }

    private String getOperationCodes(OracleConnectorConfig config) {
        if (config.isLobEnabled()) {
            if (!Strings.isNullOrEmpty(config.getPdbName())) {
                return OPERATION_CODES_PDB_LOB_ENABLED;
            }
            return OPERATION_CODES_LOB_ENABLED;
        }
        else if (!Strings.isNullOrEmpty(config.getPdbName())) {
            return OPERATION_CODES_PDB_LOB_DISABLED;
        }
        else {
            return OPERATION_CODES_LOB_DISABLED;
        }
    }

    private String getOperationCodesTemplate(OracleConnectorConfig config) {
        if (!Strings.isNullOrEmpty(config.getPdbName())) {
            return OPERATION_CODES_PDB_TEMPLATE;
        }
        return OPERATION_CODES_NON_PDB_TEMPLATE;
    }

}
