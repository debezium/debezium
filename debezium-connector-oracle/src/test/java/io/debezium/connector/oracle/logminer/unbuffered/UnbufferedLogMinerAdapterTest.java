/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.AbstractLogMinerAdapterTest;

/**
 * Unit tests for the {@link UnbufferedLogMinerAdapter} class.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_UNBUFFERED)
public class UnbufferedLogMinerAdapterTest extends AbstractLogMinerAdapterTest<UnbufferedLogMinerAdapter> {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    @Override
    protected UnbufferedLogMinerAdapter createAdapter(OracleConnectorConfig connectorConfig) {
        return Mockito.spy(new UnbufferedLogMinerAdapter(connectorConfig));
    }
}
