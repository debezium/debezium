/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Clock;

import ch.qos.logback.classic.Level;

/**
 * Unit tests for {@link PostgresReadOnlyIncrementalSnapshotChangeEventSource}.
 */
public class PostgresReadOnlyIncrementalSnapshotChangeEventSourceTest {

    @Mock
    private RelationalDatabaseConnectorConfig config;
    @Mock
    private PostgresConnection jdbcConnection;
    @Mock
    private PostgresSchema schema;
    @SuppressWarnings("rawtypes")
    @Mock
    private EventDispatcher dispatcher;
    @Mock
    private Clock clock;
    @SuppressWarnings("rawtypes")
    @Mock
    private SnapshotProgressListener progressListener;
    @SuppressWarnings("rawtypes")
    @Mock
    private DataChangeEventListener dataChangeEventListener;
    @SuppressWarnings("rawtypes")
    @Mock
    private NotificationService notificationService;

    private PostgresReadOnlyIncrementalSnapshotChangeEventSource<PostgresPartition> source;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        MockitoAnnotations.openMocks(this);
        source = new PostgresReadOnlyIncrementalSnapshotChangeEventSource<>(
                config, jdbcConnection, dispatcher, schema, clock,
                progressListener, dataChangeEventListener, notificationService);
    }

    @Test
    @FixFor("debezium/dbz#2431")
    void forceTransactionQueryIsRecoveryAware() throws Exception {
        Field field = PostgresReadOnlyIncrementalSnapshotChangeEventSource.class
                .getDeclaredField("FORCE_NEW_TRANSACTION");
        field.setAccessible(true);
        String sql = (String) field.get(null);

        assertThat(sql).contains("pg_is_in_recovery()");
        assertThat(sql).contains("pg_current_xact_id()");
    }

    @Test
    @FixFor("debezium/dbz#2431")
    void forceNewTransactionIdIsNoOpOnHotStandby() throws Exception {
        // Simulate a hot standby: pg_is_in_recovery() = true, so the CASE returns NULL
        stubQueryResult(null);

        LogInterceptor logInterceptor = new LogInterceptor(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);
        logInterceptor.setLoggerLevel(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class, Level.TRACE);

        assertThatNoException().isThrownBy(this::invokeForceNewTransactionId);
        assertThat(logInterceptor.containsMessage("Skipping transaction ID assignment on hot standby")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2431")
    void forceNewTransactionIdLogsTransactionOnPrimary() throws Exception {
        // Simulate a primary: pg_is_in_recovery() = false, so a real transaction ID is returned
        stubQueryResult("12345");

        LogInterceptor logInterceptor = new LogInterceptor(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);
        logInterceptor.setLoggerLevel(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class, Level.TRACE);

        assertThatNoException().isThrownBy(this::invokeForceNewTransactionId);
        assertThat(logInterceptor.containsMessage("Created new transaction ID 12345")).isTrue();
    }

    private void stubQueryResult(String txId) throws Exception {
        doAnswer(invocation -> {
            JdbcConnection.ResultSetConsumer consumer = invocation.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn(txId);
            consumer.accept(rs);
            return jdbcConnection;
        }).when(jdbcConnection).query(anyString(), any(JdbcConnection.ResultSetConsumer.class));
    }

    private void invokeForceNewTransactionId() throws Exception {
        Method method = PostgresReadOnlyIncrementalSnapshotChangeEventSource.class
                .getDeclaredMethod("forceNewTransactionId");
        method.setAccessible(true);
        method.invoke(source);
    }
}
