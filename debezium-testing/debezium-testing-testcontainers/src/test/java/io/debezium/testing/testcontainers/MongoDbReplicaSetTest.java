/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.MongoDbReplicaSet.replicaSet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;

/**
 * @see <a href="https://issues.redhat.com/browse/DBZ-5857">DBZ-5857</a>
 */
public class MongoDbReplicaSetTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbReplicaSetTest.class);

    @Test
    public void testCluster() throws InterruptedException {
        try (var cluster = replicaSet().build()) {
            LOGGER.info("Starting {}...", cluster);
            cluster.start();

            // Create a connection string with a desired read preference
            var readPreference = ReadPreference.primary();
            var connectionString = new ConnectionString(cluster.getConnectionString() + "/?readPreference=" + readPreference.getName());

            LOGGER.info("Connecting to cluster: {}", connectionString);
            try (var client = MongoClients.create(connectionString)) {
                LOGGER.info("Connected to cluster: {}", client.getClusterDescription());
                var collection = setup(client);

                run(cluster, client, collection);
            }
        }
    }

    private void run(MongoDbReplicaSet cluster, MongoClient client, MongoCollection<Document> collection) {
        try (var cursor = collection.watch().batchSize(1).cursor()) {
            // Write 2 docs
            collection.insertOne(Document.parse("{username: 'user" + 1 + "', name: 'User " + 1 + "'}"));
            collection.insertOne(Document.parse("{username: 'user" + 2 + "', name: 'User " + 2 + "'}"));

            // Read one
            LOGGER.info("{}", cursor.next());

            // Force a promotion that invalidates read preference on primary
            LOGGER.info("Demoting primary");
            cluster.stepDown();

            // Wait until the cursor address doesn't match the read preference (primary)
            await().atMost(30, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> cluster.tryPrimary()
                            .map(node -> !node.getNamedAddress().toString().equals(cursor.getServerAddress().toString()) &&
                                    !node.getClientAddress().toString().equals(cursor.getServerAddress().toString()))
                            .orElse(false));

            // Ensure it's invalid
            if (!MongoDbContainer.IMAGE_VERSION.equals("4.0")) {
                assertThat(isSelectedReadPreference(client, collection, cursor)).isFalse();
            }

            throw new ResumableCursorException(cursor.getResumeToken());
        }
        catch (ResumableCursorException e) {
            // Start resuming where we left off
            var resumeToken = e.resumeToken();
            try (var cursor = collection.watch().resumeAfter(resumeToken).batchSize(1).cursor()) {
                // Ensure we are now consistent
                assertThat(isSelectedReadPreference(client, collection, cursor)).isTrue();

                // Get the second document
                LOGGER.info("{}", cursor.next());
            }
        }
    }

    private static boolean isSelectedReadPreference(MongoClient client, MongoCollection<Document> collection,
                                                    MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
        // Find all remaining nodes that match our preference
        var candidates = new ReadPreferenceServerSelector(collection.getReadPreference())
                .select(client.getClusterDescription()); // Could get this from `ClusterListener` instead

        // Determine if the cursor matches any one of these candidates
        return candidates.stream()
                .map(ServerDescription::getAddress)
                .anyMatch(address -> address.equals(cursor.getServerCursor() == null ? null : cursor.getServerCursor().getAddress()));
    }

    public static class ResumableCursorException extends RuntimeException {

        private final BsonDocument resumeToken;

        ResumableCursorException(BsonDocument resumeToken) {
            this.resumeToken = resumeToken;
        }

        public BsonDocument resumeToken() {
            return resumeToken;
        }

    }

    private static MongoCollection<Document> setup(MongoClient mongoClient) throws InterruptedException {
        var database = mongoClient.getDatabase("testChangeStreams");
        database.drop();
        Thread.sleep(1000);

        // Select the collection to query.
        return database.getCollection("documents");
    }

}
