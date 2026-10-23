/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.ServerErrorMessage;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.pipeline.DataChangeEvent;

public class PostgresErrorHandlerTest {

    private static final String ROLE_AUTHORIZATION_ERROR = "FATAL: role not permitted to log in";
    private static final String PASSWORD_AUTHENTICATION_ERROR = "FATAL: password authentication failed for user debezium";
    private static final String A_CLASSIFIED_EXCEPTION = "Database connection failed when writing to copy";

    private final PostgresErrorHandler errorHandler = new PostgresErrorHandler(
            new PostgresConnectorConfig(Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "postgres")
                    .build()),
            new ChangeEventQueue.Builder<DataChangeEvent>().queueProvider(createDefaultQueueProvider(CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE)).build(), null);

    private static DefaultQueueProvider<DataChangeEvent> createDefaultQueueProvider(int maxQueueSize) {
        DefaultQueueProvider<DataChangeEvent> provider = new DefaultQueueProvider<>();
        provider.configure(java.util.Map.of("max.queue.size", String.valueOf(maxQueueSize)));
        return provider;
    }

    @Test
    void classifiedPSQLExceptionIsRetryable() {
        PSQLException testException = new PSQLException(A_CLASSIFIED_EXCEPTION, PSQLState.CONNECTION_FAILURE);
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    void nonCommunicationExceptionNotRetryable() {
        Exception testException = new NullPointerException();
        assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    @Test
    void nullThrowableIsNotRetryable() {
        assertThat(errorHandler.isRetriable(null)).isFalse();
    }

    @Test
    void encapsulatedPSQLExceptionIsRetriable() {
        Exception testException = new IllegalArgumentException(
                new PSQLException("definitely not a postgres error", PSQLState.CONNECTION_FAILURE));
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    void classifiedPSQLExceptionWrappedInDebeziumExceptionIsRetryable() {
        PSQLException psqlException = new PSQLException(A_CLASSIFIED_EXCEPTION, PSQLState.CONNECTION_FAILURE);
        DebeziumException testException = new DebeziumException(psqlException);
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    void randomUnhandledExceptionIsNotRetryable() {
        RuntimeException testException = new RuntimeException();
        assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    @Test
    void connectionFailureIsNotPermanent() {
        PSQLException psqlException = new PSQLException(A_CLASSIFIED_EXCEPTION, PSQLState.CONNECTION_FAILURE);
        assertThat(PostgresErrorHandler.isPermanentError(psqlException)).isFalse();
    }

    @Test
    void authorizationFailureIsPermanent() {
        PSQLException psqlException = new PSQLException(ROLE_AUTHORIZATION_ERROR, PSQLState.INVALID_AUTHORIZATION_SPECIFICATION);
        assertThat(PostgresErrorHandler.isPermanentError(psqlException)).isTrue();
    }

    @Test
    void invalidPasswordIsPermanent() {
        PSQLException psqlException = new PSQLException(PASSWORD_AUTHENTICATION_ERROR, PSQLState.INVALID_PASSWORD);
        assertThat(PostgresErrorHandler.isPermanentError(psqlException)).isTrue();
    }

    @Test
    void authorizationFailureWrappedInDebeziumExceptionIsPermanent() {
        // Mirrors getDatabaseCharset() wrapping PSQLException in DebeziumException during start()
        PSQLException psqlException = new PSQLException(ROLE_AUTHORIZATION_ERROR, PSQLState.INVALID_AUTHORIZATION_SPECIFICATION);
        DebeziumException wrapper = new DebeziumException("Couldn't obtain encoding for database", psqlException);
        assertThat(PostgresErrorHandler.isPermanentError(wrapper)).isTrue();
    }

    @Test
    void nonSqlExceptionIsNotPermanent() {
        assertThat(PostgresErrorHandler.isPermanentError(new RuntimeException("boom"))).isFalse();
    }

    @Test
    void nullIsNotPermanent() {
        assertThat(PostgresErrorHandler.isPermanentError(null)).isFalse();
    }

    @Test
    void slotInvalidatedDetectedFromServerErrorMessage() {
        // Message text is unrelated, so only the ServerErrorMessage detail branch can match
        var serverError = new ServerErrorMessage("SERROR\0C55000\0Mreplication failed\0D"
                + "This slot has been invalidated because it exceeded the maximum reserved size.\0");
        var psqlException = new PSQLException(serverError);
        assertThat(PostgresErrorHandler.isSlotInvalidated(psqlException)).isTrue();
    }

    @Test
    void activeReplicationSlotIsStillRetriable() {
        var e = new PSQLException("ERROR: replication slot \"debezium_slot\" is active for PID 1234", PSQLState.OBJECT_IN_USE);
        assertThat(errorHandler.isRetriable(e)).isTrue();
    }

    @Test
    void slotInvalidatedIsNotRetriable() {
        var e = new DebeziumException("Failed to start replication stream",
                new PSQLException("... has been invalidated ...", PSQLState.OBJECT_NOT_IN_STATE));
        assertThat(errorHandler.isRetriable(e)).isFalse();
    }

    @Test
    void slotInvalidatedDetectedFromCanNoLongerGetChangesMessageOnly() {
        // Covers the second message pattern without the "has been invalidated" text
        var psqlException = new PSQLException(
                "ERROR: can no longer get changes from replication slot \"debezium_slot\"",
                PSQLState.OBJECT_NOT_IN_STATE);
        assertThat(PostgresErrorHandler.isSlotInvalidated(psqlException)).isTrue();
    }

    @Test
    void activeReplicationSlotIsNotSlotInvalidated() {
        // Must keep retrying, so it must not be classified as an invalidation
        var psqlException = new PSQLException(
                "ERROR: replication slot \"debezium_slot\" is active for PID 1234",
                PSQLState.OBJECT_IN_USE);
        assertThat(PostgresErrorHandler.isSlotInvalidated(psqlException)).isFalse();
        assertThat(PostgresErrorHandler.isPermanentError(psqlException)).isFalse();
    }

    @Test
    void slotInvalidatedStandbyRecoveryConflictIsDetected() {
        // Simulates PostgreSQL 16+ standby invalidation due to recovery conflict
        String errorMessage = "ERROR: can no longer get changes from replication slot \"debezium_slot\"\n"
                + "  Detail: This slot has been invalidated because it was conflicting with recovery.";
        PSQLException psqlException = new PSQLException(errorMessage, PSQLState.OBJECT_NOT_IN_STATE);
        assertThat(PostgresErrorHandler.isSlotInvalidated(psqlException)).isTrue();
    }

    @Test
    void slotInvalidatedPrimaryWalRemovedIsDetected() {
        // Simulates Primary database invalidation due to max_slot_wal_keep_size (wal_removed)
        String errorMessage = "ERROR: can no longer get changes from replication slot \"debezium_slot\"\n"
                + "  Detail: This slot has been invalidated because required WAL segments were removed.";
        PSQLException psqlException = new PSQLException(errorMessage, PSQLState.OBJECT_NOT_IN_STATE);
        assertThat(PostgresErrorHandler.isSlotInvalidated(psqlException)).isTrue();
    }

    @Test
    void deeplyNestedSlotInvalidationExceptionIsDetected() {
        // Simulates deep nesting: RuntimeException -> DebeziumException -> PSQLException
        String errorMessage = "ERROR: can no longer get changes from replication slot \"debezium_slot\"\n"
                + "  Detail: This slot has been invalidated because it was conflicting with recovery.";
        PSQLException rootCause = new PSQLException(errorMessage, PSQLState.OBJECT_NOT_IN_STATE);
        DebeziumException middleException = new DebeziumException("Failed to start replication stream", rootCause);
        RuntimeException outerException = new RuntimeException("Change event producer error", middleException);
        assertThat(PostgresErrorHandler.isSlotInvalidated(outerException)).isTrue();
    }

    @Test
    void standardConnectionFailureIsNotSlotInvalidated() {
        // Transient network/connection failures must NOT match slot invalidation
        PSQLException psqlException = new PSQLException("Database connection failed when writing to copy", PSQLState.CONNECTION_FAILURE);
        assertThat(PostgresErrorHandler.isSlotInvalidated(psqlException)).isFalse();
    }

    @Test
    void nullAndUnrelatedExceptionsAreNotSlotInvalidated() {
        assertThat(PostgresErrorHandler.isSlotInvalidated(null)).isFalse();
        assertThat(PostgresErrorHandler.isSlotInvalidated(new NullPointerException("null pointer"))).isFalse();
        assertThat(PostgresErrorHandler.isSlotInvalidated(new DebeziumException("Generic Debezium error"))).isFalse();
    }

}
