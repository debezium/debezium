/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.kafka.connect.errors.RetriableException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * Unit tests for {@link MongoDbConnectorTask#validate} focused on how resume-token validation failures are
 * classified. A communication failure while checking the resume token must surface as a
 * {@link RetriableException} so the task restarts and reconnects instead of failing permanently.
 */
class MongoDbConnectorTaskTest {

    private Offsets<MongoDbPartition, MongoDbOffsetContext> offsetsWithExistingResumeToken() {
        MongoDbOffsetContext offset = mock(MongoDbOffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(false);
        return Offsets.of(mock(MongoDbPartition.class), offset);
    }

    private MongoDbConnectorConfig configWithLogPositionCheckEnabled() {
        MongoDbConnectorConfig config = mock(MongoDbConnectorConfig.class);
        when(config.isLogPositionCheckEnabled()).thenReturn(true);
        return config;
    }

    private MongoDbConnection connectionFailingWith(Throwable failure) {
        MongoDbConnection connection = mock(MongoDbConnection.class);
        when(connection.validateLogPosition(any(), any())).thenThrow(failure);
        return connection;
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldRetryWhenResumeTokenValidationFailsWithMongoCommunicationError() {
        // The connection error handler wraps communication failures as a DebeziumException with the original
        // MongoException as the cause; this must become retriable.
        MongoDbConnection connection = connectionFailingWith(
                new DebeziumException("Error while attempting to Checking change stream", new MongoException("connection refused")));

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isInstanceOf(RetriableException.class)
                .hasCauseInstanceOf(DebeziumException.class);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldRetryWhenResumeTokenValidationFailsWithIoError() {
        MongoDbConnection connection = connectionFailingWith(
                new DebeziumException("Error while attempting to Checking change stream", new IOException("socket closed")));

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isInstanceOf(RetriableException.class);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotRetryWhenResumeTokenIsValid() {
        MongoDbConnection connection = mock(MongoDbConnection.class);
        when(connection.validateLogPosition(any(), any())).thenReturn(true);

        assertThatNoException().isThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)));
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotRetryWhenResumeTokenValidationFailsWithNonCommunicationError() {
        DebeziumException nonRetriable = new DebeziumException("misconfiguration", new IllegalStateException("bad state"));
        MongoDbConnection connection = connectionFailingWith(nonRetriable);

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isSameAs(nonRetriable);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotRetryWhenResumeTokenValidationFailsWithAuthenticationError() {
        // MongoSecurityException is a MongoException subtype, so the auth exclusion, not the blanket rule, must win.
        MongoSecurityException authFailure = new MongoSecurityException(
                MongoCredential.createCredential("user", "admin", "secret".toCharArray()), "Exception authenticating");
        MongoDbConnection connection = connectionFailingWith(
                new DebeziumException("Error while attempting to Checking change stream", authFailure));

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isInstanceOf(DebeziumException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasCause(authFailure);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotRetryWhenResumeTokenValidationFailsWithUnauthorizedError() {
        MongoCommandException unauthorized = new MongoCommandException(
                new BsonDocument("code", new BsonInt32(13)).append("errmsg", new BsonString("not authorized")),
                new ServerAddress());
        MongoDbConnection connection = connectionFailingWith(
                new DebeziumException("Error while attempting to Checking change stream", unauthorized));

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isNotInstanceOf(RetriableException.class);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotRetryWhenResumeTokenValidationFailsWithAuthenticationFailedCode() {
        MongoCommandException authenticationFailed = new MongoCommandException(
                new BsonDocument("code", new BsonInt32(18)).append("errmsg", new BsonString("Authentication failed")),
                new ServerAddress());
        MongoDbConnection connection = connectionFailingWith(
                new DebeziumException("Error while attempting to Checking change stream", authenticationFailed));

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isNotInstanceOf(RetriableException.class);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotRetryWhenAuthenticationFailureIsWrappedBehindCommunicationError() {
        // An auth failure deeper in the cause chain must veto the retry even behind a retriable wrapper.
        MongoSecurityException authFailure = new MongoSecurityException(
                MongoCredential.createCredential("user", "admin", "secret".toCharArray()), "Exception authenticating");
        MongoDbConnection connection = connectionFailingWith(
                new DebeziumException("Error while attempting to Checking change stream",
                        new MongoException("connection reset", authFailure)));

        assertThatThrownBy(() -> new MongoDbConnectorTask().validate(
                configWithLogPositionCheckEnabled(), connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)))
                .isNotInstanceOf(RetriableException.class);
    }

    @Test
    @FixFor("debezium/dbz#67")
    void shouldNotValidateWhenLogPositionCheckDisabled() {
        MongoDbConnectorConfig config = mock(MongoDbConnectorConfig.class);
        when(config.isLogPositionCheckEnabled()).thenReturn(false);
        MongoDbConnection connection = mock(MongoDbConnection.class);

        assertThatNoException().isThrownBy(() -> new MongoDbConnectorTask().validate(
                config, connection, offsetsWithExistingResumeToken(), mock(Snapshotter.class)));
    }
}
