/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.junit;

import java.util.function.Supplier;

import io.debezium.testing.testcontainers.MongoDbDeployment;

/**
 * Representation of supported MongoDB platforms
 */
public enum MongoDbPlatform {
    MONGODB_DOCKER(MongoDbDatabaseProvider::dockerReplicaSet),
    DOCUMENT_DB(DocumentDb::new);

    public final Supplier<MongoDbDeployment> provider;

    MongoDbPlatform(Supplier<MongoDbDeployment> provider) {
        this.provider = provider;
    }

    /**
     * Returns name of this instance as represented by property value
     *
     * @return property value for this instance
     */
    public final String value() {
        return name().toLowerCase();
    }

    public static MongoDbPlatform fromValue(String value) {
        return valueOf(value.toUpperCase());
    }

}
