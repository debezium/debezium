/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.mongodb.deployment;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.UuidRepresentation;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;

import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class MongoDbTestResource implements QuarkusTestResourceLifecycleManager {

    private final MongoDbReplicaSet mongoDBContainer = MongoDbReplicaSet.replicaSet()
            .name("rs0")
            .memberCount(1)
            .authEnabled(true)
            .network(Network.newNetwork())
            .imageName(DockerImageName.parse("mirror.gcr.io/library/mongo:6.0"))
            .startupTimeout(Duration.ofSeconds(90))
            .build();

    @Override
    public Map<String, String> start() {
        mongoDBContainer.start();

        try (var client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoDBContainer.getConnectionString()))
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build())) {
            final MongoDatabase db = client.getDatabase("dbA");
            db.createCollection("a_collection");

            final MongoCollection<Document> general = db.getCollection("general");
            final MongoCollection<Document> products = db.getCollection("products");
            final MongoCollection<Document> orders = db.getCollection("orders");
            final MongoCollection<Document> users = db.getCollection("users");

            try (ClientSession session = client.startSession()) {
                session.startTransaction();
                general.insertMany(session,
                        List.of(
                                new Document("key", "value1"),
                                new Document("key", "value2"),
                                new Document("key", "value3")),
                        new InsertManyOptions().bypassDocumentValidation(true));
                products.insertMany(session,
                        List.of(new Document("name", "t-shirt"),
                                new Document("name", "smartphone")),
                        new InsertManyOptions().bypassDocumentValidation(true));

                users.insertMany(session,
                        List.of(
                                new Document(
                                        Map.of(
                                                "id", 1,
                                                "name", "giovanni",
                                                "description", "developer"
                                        )),
                                new Document(
                                        Map.of(
                                                "id", 2,
                                                "name", "mario",
                                                "description", "developer"
                                        ))),
                        new InsertManyOptions().bypassDocumentValidation(true));

                orders.insertMany(session,
                        List.of(
                                new Document(Map.of(
                                        "key", 1,
                                        "name", "one")),
                                new Document(Map.of(
                                        "key", 2,
                                        "name", "two"))),
                        new InsertManyOptions().bypassDocumentValidation(true));

                session.commitTransaction();
            }
        }

        Map<String, String> config = new HashMap<>();
        config.put("quarkus.mongodb.connection-string", mongoDBContainer.getConnectionString());

        return config;
    }

    @Override
    public void stop() {
        mongoDBContainer.stop();
    }
}
