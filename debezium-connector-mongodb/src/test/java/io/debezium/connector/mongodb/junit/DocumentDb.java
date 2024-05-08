/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.junit;

import org.bson.BsonDocument;

import io.debezium.connector.mongodb.TestHelper;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.testing.testcontainers.util.FakeDns;

/**
 * DocumentDb representation of {@link MongoDbDeployment}
 */
public class DocumentDb extends MongoDbExternal {
    public DocumentDb() {
        super();
    }

    public DocumentDb(String connectionString) {
        super(connectionString);
    }

    @Override
    public void start() {
        // DocumentDB requires SSH tunneling and custom DNS resolution
        var dns = FakeDns.getInstance().install();
        getHosts().forEach(DockerUtils::addFakeDnsEntry);

        // Enable change streams
        try (var client = TestHelper.connect(this)) {
            client.getDatabase("admin").runCommand(
                    BsonDocument.parse("{ modifyChangeStreams: 1, database: '', collection: '', enable: true }"));
        }
    }
}
