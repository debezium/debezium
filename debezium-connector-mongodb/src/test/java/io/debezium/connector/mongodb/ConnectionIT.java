/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.util.Testing;

public class ConnectionIT extends AbstractMongoIT {

    @Before
    public void setUp() {
        TestHelper.cleanDatabase(primary, "dbA");
        TestHelper.cleanDatabase(primary, "dbB");
        TestHelper.cleanDatabase(primary, "dbC");
    }

    @Test
    public void shouldCreateMovieDatabase() {
        useConfiguration(config.edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "dbA,dbB")
                .with(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST, "dbB.moviesB")
                .build());

        Testing.print("Configuration: " + config);

        List<String> dbNames = Arrays.asList("A", "B", "C");

        primary.execute("shouldCreateMovieDatabases", mongo -> {
            Testing.debug("Getting or creating 'movies' collections");

            for (String dbName : dbNames) {
                // Create a database and a collection in that database ...
                MongoDatabase db = mongo.getDatabase("db" + dbName);

                // Get or create a collection in that database ...
                db.getCollection("movies" + dbName);
            }

            Testing.debug("Completed getting 'movies' collections");
        });

        primary.execute("Add document to movies collections", mongo -> {
            Testing.debug("Adding document to 'movies' collections");

            for (String dbName : dbNames) {
                // Add a document to that collection ...
                MongoDatabase db = mongo.getDatabase("db" + dbName);
                MongoCollection<Document> collection = db.getCollection("movies" + dbName);
                MongoCollection<Document> movies = collection;
                InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
                movies.insertOne(Document.parse("{ \"name\":\"Starter Wars\"}"), insertOptions);
                assertThat(collection.countDocuments()).isEqualTo(1);

                // Read the collection to make sure we can find our document ...
                Bson filter = Filters.eq("name", "Starter Wars");
                FindIterable<Document> movieResults = collection.find(filter);
                try (MongoCursor<Document> cursor = movieResults.iterator();) {
                    assertThat(cursor.tryNext().getString("name")).isEqualTo("Starter Wars");
                    assertThat(cursor.tryNext()).isNull();
                }
            }

            Testing.debug("Completed adding documents to 'movies' collections");
        });

        // Now that we've put at least one document into our collection, verify we can see the database and collection ...
        assertThat(primary.databaseNames()).containsOnly("dbA", "dbB");
        assertThat(primary.collections()).containsOnly(new CollectionId(replicaSet.replicaSetName(), "dbA", "moviesA"));

        // Read oplog from beginning ...
        List<Document> eventQueue = new LinkedList<>();
        int minimumEventsExpected = 1;
        long maxSeconds = 5;
        primary.execute("read oplog from beginning", mongo -> {
            Testing.debug("Getting local.oplog.rs");

            BsonTimestamp oplogStart = new BsonTimestamp(1, 1);
            Bson filter = Filters.and(Filters.gt("ts", oplogStart), // start just after our last position
                    Filters.exists("fromMigrate", false)); // skip internal movements across shards
            FindIterable<Document> results = mongo.getDatabase("local")
                    .getCollection("oplog.rs")
                    .find(filter)
                    .sort(new Document("$natural", 1))
                    .oplogReplay(true) // tells Mongo to not rely on indexes
                    .noCursorTimeout(true) // don't timeout waiting for events
                    .cursorType(CursorType.TailableAwait);

            Testing.debug("Reading local.oplog.rs");
            try (MongoCursor<Document> cursor = results.iterator();) {
                Document event = null;
                long stopTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxSeconds);
                while (System.currentTimeMillis() < stopTime && eventQueue.size() < minimumEventsExpected) {
                    while ((event = cursor.tryNext()) != null) {
                        eventQueue.add(event);
                    }
                }
                assertThat(eventQueue.size()).isGreaterThanOrEqualTo(1);
            }
            Testing.debug("Completed local.oplog.rs");
        });

        eventQueue.forEach(event -> {
            Testing.print("Found: " + event);
            BsonTimestamp position = event.get("ts", BsonTimestamp.class);
            assert position != null;
        });
    }

    @Test
    public void shouldListDatabases() {
        Testing.Print.enable();
        Testing.print("Databases: " + primary.databaseNames());
    }
}
