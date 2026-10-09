/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.UuidRepresentation;
import org.junit.jupiter.api.Test;

import com.mongodb.ReadPreference;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.DefaultMongoDbAuthProvider;
import io.debezium.connector.mongodb.connection.client.DefaultMongoDbClientFactory;

public class MongoDbClientFactoryTest {
    private Configuration config(ConnectionResourceTracker tracker) {
        return tracker.track(TestHelper.getConfiguration("mongodb://localhost:27017/"));
    }

    @Test
    void shouldReleaseAuthenticationResourcesOnce() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var factory = new DefaultMongoDbClientFactory(config(tracker));
            factory.close();
            factory.close();
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseResourcesWhenAuthenticationInitializationFails() {
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.initFailure = new IllegalStateException("Authentication initialization failed");
            assertThatThrownBy(() -> new DefaultMongoDbClientFactory(config(tracker))).isSameAs(tracker.initFailure);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseResourcesWhenSettingsCreationFails() {
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.settingsFailure = new IllegalArgumentException("Authentication settings failed");
            assertThatThrownBy(() -> new DefaultMongoDbClientFactory(config(tracker))).isSameAs(tracker.settingsFailure);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldReleaseResourcesWhenTrustStoreCannotBeLoaded() {
        try (var tracker = new ConnectionResourceTracker()) {
            final var config = config(tracker).edit()
                    .with(MongoDbConnectorConfig.SSL_TRUSTSTORE, "target/missing-auth-lifecycle-truststore")
                    .build();
            assertThatThrownBy(() -> new DefaultMongoDbClientFactory(config)).hasRootCauseInstanceOf(java.nio.file.NoSuchFileException.class);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldPreserveInitializationFailureWhenCleanupFails() {
        try (var tracker = new ConnectionResourceTracker()) {
            tracker.initFailure = new IllegalStateException("Authentication initialization failed");
            tracker.closeFailure = new IllegalStateException("Authentication cleanup failed");
            assertThatThrownBy(() -> new DefaultMongoDbClientFactory(config(tracker)))
                    .isSameAs(tracker.initFailure)
                    .hasSuppressedException(tracker.closeFailure);
            tracker.assertReleased();
        }
    }

    @Test
    void shouldPreserveConnectionStringPrecedence() {
        final var config = TestHelper.getConfiguration("mongodb://uriUser:uriPassword@localhost:27017/uriAuth"
                + "?connectTimeoutMS=1234&socketTimeoutMS=2345&serverSelectionTimeoutMS=3456&readPreference=secondary&tls=false")
                .edit()
                .with(MongoDbConnectorConfig.USER, "propertyUser")
                .with(MongoDbConnectorConfig.PASSWORD, "propertyPassword")
                .with(MongoDbConnectorConfig.AUTH_SOURCE, "propertyAuth")
                .with(MongoDbConnectorConfig.CONNECT_TIMEOUT_MS, 9000)
                .with(MongoDbConnectorConfig.SOCKET_TIMEOUT_MS, 9000)
                .with(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS, 9000)
                .with(MongoDbConnectorConfig.SSL_ENABLED, true)
                .build();
        try (var factory = new DefaultMongoDbClientFactory(config)) {
            final var settings = factory.getMongoClientSettings();
            assertThat(settings.getCredential().getUserName()).isEqualTo("uriUser");
            assertThat(settings.getCredential().getSource()).isEqualTo("uriAuth");
            assertThat(settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS)).isEqualTo(1234);
            assertThat(settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS)).isEqualTo(2345);
            assertThat(settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(3456);
            assertThat(settings.getSslSettings().isEnabled()).isFalse();
            assertThat(settings.getReadPreference()).isEqualTo(ReadPreference.secondary());
            assertThat(settings.getUuidRepresentation()).isEqualTo(UuidRepresentation.STANDARD);
        }
    }

    @Test
    void shouldOnlyInstantiateAuthenticationProviderWhenNeeded() {
        CountingAuthProvider.instances.set(0);
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.AUTH_PROVIDER_CLASS, CountingAuthProvider.class)
                .build();
        new MongoDbConnectorConfig(config);
        try (var task = new MongoDbTaskContext(config)) {
            assertThat(CountingAuthProvider.instances).hasValue(0);
            task.getConnectionContext();
            task.getConnectionContext();
            assertThat(CountingAuthProvider.instances).hasValue(1);
        }
    }

    @Test
    void shouldWaitForAllClientsBeforeClosingAuthentication() {
        try (var tracker = new ConnectionResourceTracker()) {
            try (var factory = new DefaultMongoDbClientFactory(config(tracker));
                    var first = factory.openClient();
                    var second = factory.openClient()) {
                factory.close();
                assertThatThrownBy(factory::openClient).isInstanceOf(IllegalStateException.class);
                tracker.assertAuthenticationActive();
                first.close();
                first.close();
                tracker.assertAuthenticationActive();
                second.close();
                tracker.assertReleased();
            }
        }
    }

    @Test
    void shouldCloseDriverResourcesWithoutClearingCallerInterrupt() {
        try (var tracker = new ConnectionResourceTracker()) {
            try (var factory = new DefaultMongoDbClientFactory(config(tracker)); var client = factory.openClient()) {
                Thread.currentThread().interrupt();
                client.close();
                factory.close();
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            }
            finally {
                Thread.interrupted();
            }
            tracker.assertClientsCreated();
            tracker.assertReleased();
        }
    }

    public static class CountingAuthProvider extends DefaultMongoDbAuthProvider {
        private static final AtomicInteger instances = new AtomicInteger();

        public CountingAuthProvider() {
            instances.incrementAndGet();
        }
        // Deliberately inherits the default close implementation, like an existing custom provider.
    }
}
