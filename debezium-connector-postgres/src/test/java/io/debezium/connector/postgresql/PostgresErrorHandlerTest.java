/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

public class PostgresErrorHandlerTest {
    private static final String A_CLASSIFIED_EXCEPTION = "Database connection failed when writing to copy";

    private final PostgresErrorHandler errorHandler = new PostgresErrorHandler(
            new PostgresConnectorConfig(Configuration.create()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, "postgres")
                    .build()),
            new ChangeEventQueue.Builder<DataChangeEvent>().build());

    @Test
    public void classifiedPSQLExceptionIsRetryable() {
        PSQLException testException = new PSQLException(A_CLASSIFIED_EXCEPTION, PSQLState.CONNECTION_FAILURE);
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    public void nonCommunicationExceptionNotRetryable() {
        Exception testException = new NullPointerException();
        assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    @Test
    public void nullThrowableIsNotRetryable() {
        assertThat(errorHandler.isRetriable(null)).isFalse();
    }

    @Test
    public void encapsulatedPSQLExceptionIsRetriable() {
        Exception testException = new IllegalArgumentException(
                new PSQLException("definitely not a postgres error", PSQLState.CONNECTION_FAILURE));
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    public void classifiedPSQLExceptionWrappedInDebeziumExceptionIsRetryable() {
        PSQLException psqlException = new PSQLException(A_CLASSIFIED_EXCEPTION, PSQLState.CONNECTION_FAILURE);
        DebeziumException testException = new DebeziumException(psqlException);
        assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    public void randomUnhandledExceptionIsNotRetryable() {
        RuntimeException testException = new RuntimeException();
        assertThat(errorHandler.isRetriable(testException)).isFalse();
    }
}
