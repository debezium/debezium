/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;

import io.debezium.annotation.ThreadSafe;

/**
 * A connection pool of MongoClient instances. This pool supports creating clients that communicate explicitly with a single
 * server, or clients that communicate with any members of a replica set or sharded cluster given a set of seed addresses.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class MongoClients {

    /**
     * Obtain a builder that can be used to configure and {@link Builder#build() create} a connection pool.
     *
     * @return the new builder; never null
     */
    public static Builder create() {
        return new Builder();
    }

    /**
     * Configures and builds a ConnectionPool.
     */
    public static class Builder {
        private final MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

        /**
         * Add the given {@link MongoCredential} for use when creating clients.
         *
         * @param credential the credential; may be {@code null}, though this method does nothing if {@code null}
         * @return this builder object so methods can be chained; never null
         */
        public Builder withCredential(MongoCredential credential) {
            if (credential != null) {
                settingsBuilder.credential(credential);
            }
            return this;
        }

        /**
         * Obtain the options builder for client connections.
         *
         * @return the option builder; never null
         */
        public MongoClientSettings.Builder settings() {
            return settingsBuilder;
        }

        /**
         * Build the client pool that will use the credentials and options already configured on this builder.
         *
         * @return the new client pool; never null
         */
        public MongoClients build() {
            return new MongoClients(settingsBuilder);
        }
    }

    private final Map<MongoClientSettings, MongoClient> connections = new ConcurrentHashMap<>();

    private final MongoClientSettings defaultSettings;

    private MongoClients(MongoClientSettings.Builder settings) {
        this.defaultSettings = settings.build();
    }

    /**
     * Creates fresh {@link MongoClientSettings.Builder} from {@link #defaultSettings}
     * @return connection settings builder
     */
    protected MongoClientSettings.Builder settings() {
        return MongoClientSettings.builder(defaultSettings);
    }

    /**
     * Clear out and close any open connections.
     */
    public void clear() {
        connections.values().forEach(MongoClient::close);
        connections.clear();
    }

    public MongoClient client(ConnectionString connectionString) {
        return client(settings -> settings.applyConnectionString(connectionString));
    }

    public MongoClient client(ReplicaSet replicaSet, ReadPreference preference) {
        return client(settings -> settings
                .applyConnectionString(replicaSet.connectionString())
                .readPreference(preference));
    }

    protected MongoClient client(Consumer<MongoClientSettings.Builder> configurator) {
        MongoClientSettings.Builder settings = settings();
        configurator.accept(settings);

        return connections.computeIfAbsent(settings.build(), com.mongodb.client.MongoClients::create);
    }
}
