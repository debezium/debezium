/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.util.function.Consumer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;

import io.debezium.annotation.ThreadSafe;

/**
 * A connection pool of MongoClient instances.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public final class MongoDbClientFactory {

    private final MongoClientSettings defaultSettings;

    /**
     * Obtains new client factory
     *
     * @param configurator settings instance use as template for all clients
     * @return mongo client factory
     */
    public static MongoDbClientFactory create(Consumer<MongoClientSettings.Builder> configurator) {
        var settings = MongoClientSettings.builder();
        configurator.accept(settings);
        return new MongoDbClientFactory(settings);
    }

    private MongoDbClientFactory(MongoClientSettings.Builder settings) {
        this.defaultSettings = settings.build();
    }

    /**
     * Creates fresh {@link MongoClientSettings.Builder} from {@link #defaultSettings}
     * @return connection settings builder
     */
    private MongoClientSettings.Builder settings() {
        return MongoClientSettings.builder(defaultSettings);
    }

    public MongoClient client(ConnectionString connectionString) {
        return client(settings -> settings.applyConnectionString(connectionString));
    }

    public MongoClient client(ReplicaSet replicaSet, ReadPreference preference) {
        return client(settings -> settings
                .applyConnectionString(replicaSet.connectionString())
                .readPreference(preference));
    }

    private MongoClient client(Consumer<MongoClientSettings.Builder> configurator) {
        MongoClientSettings.Builder settings = settings();
        configurator.accept(settings);

        return com.mongodb.client.MongoClients.create(settings.build());
    }
}
