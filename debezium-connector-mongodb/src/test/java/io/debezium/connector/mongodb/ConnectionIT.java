/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.util.Testing;

public class ConnectionIT extends AbstractMongoIT {

    @Before
    public void setUp() {
        TestHelper.cleanDatabase(mongo, "dbA");
        TestHelper.cleanDatabase(mongo, "dbB");
        TestHelper.cleanDatabase(mongo, "dbC");
    }

    @Test
    public void shouldHonorReadPreference() throws InterruptedException {

        connection.execute("Check client read preference", (MongoClient mongo) -> {
            if (mongo instanceof MongoClientImpl) {
                var settings = ((MongoClientImpl) mongo).getSettings();
                assertThat(settings.getReadPreference()).isEqualTo(ReadPreference.secondaryPreferred());
            }
        });
    }

    @Test(expected = DebeziumException.class)
    public void shouldUseSSL() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        useConfiguration(config.edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.SSL_ENABLED, true)
                .with(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS, 2000)
                .build());

        connection.execute("Try SSL connection", mongo -> {
            connection.close();
            mongo.getDatabase("dbit").listCollectionNames().first();
        });
    }

    @Test
    public void shouldCreateMovieDatabase() throws InterruptedException {
        useConfiguration(config.edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "dbA,dbB")
                .with(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST, "dbB.moviesB")
                .build());

        Testing.print("Configuration: " + config);

        List<String> dbNames = Arrays.asList("A", "B", "C");

        try (var client = connect()) {
            Testing.debug("Getting or creating 'movies' collections");

            for (String dbName : dbNames) {
                // Create a database and a collection in that database ...
                MongoDatabase db = client.getDatabase("db" + dbName);

                // Get or create a collection in that database ...
                db.getCollection("movies" + dbName);
            }

            Testing.debug("Completed getting 'movies' collections");
        }

        try (var client = connect()) {
            Testing.debug("Adding document to 'movies' collections");

            for (String dbName : dbNames) {
                // Add a document to that collection ...
                MongoDatabase db = client.getDatabase("db" + dbName);
                MongoCollection<Document> collection = db.getCollection("movies" + dbName);
                MongoCollection<Document> movies = collection;
                InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
                movies.insertOne(Document.parse("{ \"name\":\"Starter Wars\"}"), insertOptions);
                assertThat(collection.countDocuments()).isEqualTo(1);
            }
            Testing.debug("Completed adding documents to 'movies' collections");
        }

        connection.execute("Add document to movies collections", client -> {
            for (String dbName : dbNames) {
                // Read the collection to make sure we can find our document ...
                MongoDatabase db = client.getDatabase("db" + dbName);
                MongoCollection<Document> collection = db.getCollection("movies" + dbName);
                Bson filter = Filters.eq("name", "Starter Wars");
                FindIterable<Document> movieResults = collection.find(filter);
                try (MongoCursor<Document> cursor = movieResults.iterator();) {
                    assertThat(cursor.tryNext().getString("name")).isEqualTo("Starter Wars");
                    assertThat(cursor.tryNext()).isNull();
                }
            }
        });

        // Now that we've put at least one document into our collection, verify we can see the database and collection ...
        assertThat(connection.databaseNames()).containsOnly("dbA", "dbB");
        assertThat(connection.collections()).containsOnly(new CollectionId("dbA", "moviesA"));
    }
}
