/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import com.mongodb.MongoClientSettings.Builder;

import io.debezium.config.Configuration;

/**
 * An interface that defines the MongoDB Authentication strategy.
 *
 */
public interface MongoDbAuthProvider {
    /**
     * Initializes the provider.
     * Called on MongoDB connector initialization.
     */
    void init(Configuration config);

    /**
     * Setups authentication configuration on existing {@link com.mongodb.MongoClientSettings.Builder}.
     * Called before every connection creation.
     * @param builder the builder with all configurations
     * @return The builder with authentication configuration applied
     */
    Builder addAuthConfig(Builder builder);
}
