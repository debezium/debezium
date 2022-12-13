/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.MongoDbShardedCluster.shardedCluster;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.ListAssert;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

import io.debezium.testing.testcontainers.util.DockerUtils;

/**
 * @see <a href="https://issues.redhat.com/browse/DBZ-5857">DBZ-5857</a>
 */
@Disabled
public class MongoDbShardedClusterTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(MongoDbShardedClusterTest.class);

    @BeforeAll
    static void setupAll() {
        DockerUtils.enableFakeDnsIfRequired();
    }

    @AfterAll
    static void tearDownAll() {
        DockerUtils.disableFakeDns();
    }

    @Test
    public void testCluster() {
        try (var cluster = shardedCluster().shardCount(1).replicaCount(1).routerCount(1).build()) {
            LOGGER.info("Starting {}...", cluster);
            cluster.start();

            // Create a connection string with a desired read preference
            var readPreference = ReadPreference.primary();
            var connectionString = new ConnectionString(cluster.getConnectionString() + "/?readPreference=" + readPreference.getName());

            LOGGER.info("Connecting to cluster: {}", connectionString);
            try (var client = MongoClients.create(connectionString)) {
                LOGGER.info("Connected to cluster: {}", client.getClusterDescription());

                var databaseName = "test";
                cluster.enableSharding(databaseName); // Only needed in 5.0, no-op in other versions

                var collectionName = "docs";
                cluster.shardCollection(databaseName, collectionName, "name");

                var database = client.getDatabase(databaseName);
                assertThatShards(client).hasSize(1);

                // Populate the collection
                var collection = database.getCollection(collectionName);
                int docCount = 10;
                rangeClosed(1, docCount)
                        .mapToObj(i -> Document.parse("{name:" + i + "}"))
                        .forEach(collection::insertOne);
                assertThatCollection(collection).hasSize(docCount);

                // Add another shard (2 total)
                cluster.addShard();
                assertThatShards(client).hasSize(2);

                // Remove the last shard
                cluster.removeShard();
                assertThatShards(client).hasSize(1);
            }
        }
    }

    private static ListAssert<Document> assertThatCollection(MongoCollection<Document> collection) {
        return assertThat(stream(collection.find().spliterator(), false)
                .collect(toList()));
    }

    private static ListAssert<Document> assertThatShards(MongoClient client) {
        return assertThat(client
                .getDatabase("admin")
                .runCommand(new BasicDBObject("listShards", 1))
                .getList("shards", Document.class));
    }

}
