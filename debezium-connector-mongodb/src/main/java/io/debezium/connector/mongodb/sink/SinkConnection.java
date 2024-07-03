/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;

import io.debezium.config.Field;

public final class SinkConnection {

    public static Optional<MongoClient> canConnect(final Config config, final Field connectionStringConfigName) {
        Optional<ConfigValue> optionalConnectionString = getConfigByName(config, connectionStringConfigName.name());
        if (optionalConnectionString.isPresent() && optionalConnectionString.get().errorMessages().isEmpty()) {
            ConfigValue configValue = optionalConnectionString.get();

            AtomicBoolean connected = new AtomicBoolean();
            CountDownLatch latch = new CountDownLatch(1);
            ConnectionString connectionString = new ConnectionString((String) configValue.value());
            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .applyToClusterSettings(
                            b -> b.addClusterListener(
                                    new ClusterListener() {
                                        @Override
                                        public void clusterOpening(final ClusterOpeningEvent event) {
                                        }

                                        @Override
                                        public void clusterClosed(final ClusterClosedEvent event) {
                                        }

                                        @Override
                                        public void clusterDescriptionChanged(
                                                                              final ClusterDescriptionChangedEvent event) {
                                            ReadPreference readPreference = connectionString.getReadPreference() != null
                                                    ? connectionString.getReadPreference()
                                                    : ReadPreference.primaryPreferred();
                                            if (!connected.get()
                                                    && event.getNewDescription().hasReadableServer(readPreference)) {
                                                connected.set(true);
                                                latch.countDown();
                                            }
                                        }
                                    }))
                    .build();

            long latchTimeout = mongoClientSettings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS) + 500;
            MongoClient mongoClient = MongoClients.create(mongoClientSettings);

            try {
                if (!latch.await(latchTimeout, TimeUnit.MILLISECONDS)) {
                    configValue.addErrorMessage("Unable to connect to the server.");
                    mongoClient.close();
                }
            }
            catch (InterruptedException e) {
                mongoClient.close();
                throw new ConnectException(e);
            }

            if (configValue.errorMessages().isEmpty()) {
                return Optional.of(mongoClient);
            }
        }
        return Optional.empty();
    }

    public static Optional<ConfigValue> getConfigByName(final Config config, final String name) {
        for (final ConfigValue configValue : config.configValues()) {
            if (configValue.name().equals(name)) {
                return Optional.of(configValue);
            }
        }
        return Optional.empty();
    }

    private SinkConnection() {
    }
}
