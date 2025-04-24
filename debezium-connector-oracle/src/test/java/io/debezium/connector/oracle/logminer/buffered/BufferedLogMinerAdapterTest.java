/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.AbstractLogMinerAdapterTest;

/**
 * Unit tests for the {@link BufferedLogMinerAdapter} class.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class BufferedLogMinerAdapterTest extends AbstractLogMinerAdapterTest<BufferedLogMinerAdapter> {

    protected BufferedLogMinerAdapter createAdapter(OracleConnectorConfig connectorConfig) {
        return Mockito.spy(new BufferedLogMinerAdapter(connectorConfig));
    }
}
