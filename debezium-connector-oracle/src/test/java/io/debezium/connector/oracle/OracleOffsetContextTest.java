/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;

/**
 * Unit test that validates the behavior of the {@link OracleOffsetContext} and its friends.
 *
 * @author Chris Cranford
 */
public class OracleOffsetContextTest {

    private OracleConnectorConfig connectorConfig;
    private OracleOffsetContext.Loader offsetLoader;

    @Before
    public void beforeEach() throws Exception {
        this.connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new OracleOffsetContext.Loader(connectorConfig, TestHelper.adapter());
    }

    @Test
    @FixFor("DBZ-2994")
    public void shouldreadScnAndCommitScnAsLongValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, 12345L);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, 23456L);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            assertThat(offsetContext.getCommitScn()).isEqualTo(Scn.valueOf("23456"));
        }
    }

    @Test
    @FixFor("DBZ-2994")
    public void shouldReadScnAndCommitScnAsStringValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, "12345");
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, "23456");

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isEqualTo(Scn.valueOf("12345"));
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            assertThat(offsetContext.getCommitScn()).isEqualTo(Scn.valueOf("23456"));
        }
    }

    @Test
    @FixFor("DBZ-2994")
    public void shouldHandleNullScnAndCommitScnValues() throws Exception {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.SCN_KEY, null);
        offsetValues.put(SourceInfo.COMMIT_SCN_KEY, null);

        final OracleOffsetContext offsetContext = (OracleOffsetContext) offsetLoader.load(offsetValues);
        assertThat(offsetContext.getScn()).isNull();
        assertThat(offsetContext.getCommitScn()).isNull();
    }
}
