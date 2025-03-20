/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.util.HexConverter;

/**
 * Unit tests for the {@link LogMinerAdapter} class.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
public class LogMinerAdapterTest {

    @Test
    @FixFor("DBZ-8141")
    public void shouldCaptureInProgressTransactionStartedOnSnapshotScnBoundary() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8141")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final LogMinerAdapter adapter = createAdapter(connectorConfig);
        final List<LogFile> logs = List.of(new LogFile("abc", Scn.valueOf(20798000), Scn.MAX, BigInteger.valueOf(12345L), LogFile.Type.REDO, 1));

        // Mock up adapter methods
        Mockito.doReturn(Scn.valueOf(20798000)).when(adapter).getOldestScnAvailableInLogs(Mockito.any(), Mockito.any());
        Mockito.doReturn(logs).when(adapter).getOrderedLogsFromScn(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doNothing().when(adapter).addLogsToSession(Mockito.any(), Mockito.any());
        Mockito.doNothing().when(adapter).startSession(Mockito.any());

        final Connection connection = Mockito.mock(Connection.class);
        final Statement statement = Mockito.mock(Statement.class);

        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true, false);
        Mockito.when(rs.getBytes("XID")).thenReturn(HexConverter.convertFromHex("ABCD"));
        Mockito.when(rs.getString("START_SCN")).thenReturn("20798317");

        final OracleConnection oracleConnection = Mockito.mock(OracleConnection.class);
        Mockito.when(oracleConnection.connection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(rs);
        Mockito.when(oracleConnection.query(Mockito.any(), Mockito.any())).thenCallRealMethod();
        Mockito.when(oracleConnection.query(Mockito.any(), Mockito.any(), Mockito.any())).thenCallRealMethod();

        final Map<String, Scn> pendingTransactions = new LinkedHashMap<>();

        final Scn currentScn = Scn.valueOf("20798317");
        adapter.getPendingTransactionsFromLogs(oracleConnection, currentScn, pendingTransactions);

        assertThat(pendingTransactions).containsExactly(entry("abcd", Scn.valueOf(20798317)));
    }

    private static LogMinerAdapter createAdapter(OracleConnectorConfig connectorConfig) {
        return Mockito.spy(new LogMinerAdapter(connectorConfig));
    }
}
