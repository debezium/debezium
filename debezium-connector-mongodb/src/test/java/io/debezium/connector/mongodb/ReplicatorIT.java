/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.mongodb.util.JSON;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.data.Envelope.Operation;
import io.debezium.data.VerifyRecord;
import io.debezium.util.Testing;

public class ReplicatorIT extends AbstractMongoIT {

    @Test
    public void shouldReplicateContent() throws InterruptedException {
        Testing.Print.disable();

        // Update the configuration to add a collection filter ...
        useConfiguration(config.edit()
                               .with(MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS, 1)
                               .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, "dbA.contacts")
                               .build());

        // ------------------------------------------------------------------------------
        // ADD A DOCUMENT
        // ------------------------------------------------------------------------------
        // Add a document to the 'contacts' database ...
        primary.execute("shouldCreateContactsDatabase", mongo -> {
            Testing.debug("Populating the 'dbA.contacts' collection");

            // Create a database and a collection in that database ...
            MongoDatabase db = mongo.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(Document.parse("{ \"name\":\"Jon Snow\"}"), insertOptions);
            assertThat(db.getCollection("contacts").count()).isEqualTo(1);

            // Read the collection to make sure we can find our document ...
            Bson filter = Filters.eq("name", "Jon Snow");
            FindIterable<Document> movieResults = db.getCollection("contacts").find(filter);
            try (MongoCursor<Document> cursor = movieResults.iterator();) {
                assertThat(cursor.tryNext().getString("name")).isEqualTo("Jon Snow");
                assertThat(cursor.tryNext()).isNull();
            }
            Testing.debug("Completed document to 'dbA.contacts' collection");
        });

        // Start the replicator ...
        List<SourceRecord> records = new LinkedList<>();
        Replicator replicator = new Replicator(context, replicaSet, records::add);
        Thread thread = new Thread(replicator::run);
        thread.start();

        // Sleep for 2 seconds ...
        Thread.sleep(2000);

        // ------------------------------------------------------------------------------
        // ADD A SECOND DOCUMENT
        // ------------------------------------------------------------------------------
        // Add more documents to the 'contacts' database ...
        final Object[] expectedNames = { "Jon Snow", "Sally Hamm" };
        primary.execute("shouldCreateContactsDatabase", mongo -> {
            Testing.debug("Populating the 'dbA.contacts' collection");

            // Create a database and a collection in that database ...
            MongoDatabase db = mongo.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            contacts.insertOne(Document.parse("{ \"name\":\"Sally Hamm\"}"), insertOptions);
            assertThat(db.getCollection("contacts").count()).isEqualTo(2);

            // Read the collection to make sure we can find our documents ...
            FindIterable<Document> movieResults = db.getCollection("contacts").find();
            Set<String> foundNames = new HashSet<>();
            try (MongoCursor<Document> cursor = movieResults.iterator();) {
                while (cursor.hasNext()) {
                    String name = cursor.next().getString("name");
                    foundNames.add(name);
                }
            }
            assertThat(foundNames).containsOnly(expectedNames);
            Testing.debug("Completed document to 'dbA.contacts' collection");
        });

        // For for a minimum number of events or max time ...
        int numEventsExpected = 2; // both documents
        long stop = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3);
        while (records.size() < numEventsExpected && System.currentTimeMillis() < stop) {
            Thread.sleep(100);
        }

        // ------------------------------------------------------------------------------
        // STOP REPLICATOR AND VERIFY WE FOUND A TOTAL OF 2 EVENTS
        // ------------------------------------------------------------------------------
        replicator.stop();

        // Verify each record is valid and that we found the two records we expect ...
        final Set<String> foundNames = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);
            Struct value = (Struct) record.value();
            String after = value.getString("after");
            Document afterDoc = Document.parse(after);
            foundNames.add(afterDoc.getString("name"));
            Operation op = Operation.forCode(value.getString("op"));
            assertThat(op == Operation.READ || op == Operation.CREATE).isTrue();
        });
        assertThat(records.size()).isEqualTo(2);
        assertThat(foundNames).containsOnly(expectedNames);

        // ------------------------------------------------------------------------------
        // RESTART REPLICATOR FROM SAME POSITON
        // ------------------------------------------------------------------------------
        reuseConfiguration(config);

        // Start the replicator again ...
        records = new LinkedList<>();
        replicator = new Replicator(context, replicaSet, records::add);
        thread = new Thread(replicator::run);
        thread.start();

        // Sleep for 2 seconds ...
        Thread.sleep(2000);

        // Stop the replicator ...
        replicator.stop();

        // We should not have found any new records ...
        records.forEach(record -> {
            VerifyRecord.isValid(record);
        });
        assertThat(records.isEmpty()).isTrue();

        // ------------------------------------------------------------------------------
        // START REPLICATOR AND ALSO REMOVE A DOCUMENT
        // ------------------------------------------------------------------------------
        // Update the configuration and don't use a collection filter ...
        reuseConfiguration(config.edit()
                                 .with(MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS, 1)
                                 .build());

        // Start the replicator again ...
        records = new LinkedList<>();
        replicator = new Replicator(context, replicaSet, records::add);
        thread = new Thread(replicator::run);
        thread.start();

        // Sleep for 2 seconds ...
        Thread.sleep(2000);

        // Remove Jon Snow ...
        AtomicReference<ObjectId> jonSnowId = new AtomicReference<>();
        primary.execute("removeJonSnow", mongo -> {
            MongoDatabase db = mongo.getDatabase("dbA");
            MongoCollection<Document> contacts = db.getCollection("contacts");

            // Read the collection to make sure we can find our document ...
            Bson filter = Filters.eq("name", "Jon Snow");
            FindIterable<Document> movieResults = db.getCollection("contacts").find(filter);
            try (MongoCursor<Document> cursor = movieResults.iterator();) {
                Document doc = cursor.tryNext();
                assertThat(doc.getString("name")).isEqualTo("Jon Snow");
                assertThat(cursor.tryNext()).isNull();
                jonSnowId.set(doc.getObjectId("_id"));
                assertThat(jonSnowId.get()).isNotNull();
            }

            // Remove the document by filter ...
            contacts.deleteOne(Filters.eq("name", "Jon Snow"));
            Testing.debug("Removed the Jon Snow document from 'dbA.contacts' collection");
        });

        // For for a minimum number of events or max time ...
        numEventsExpected = 1; // just one delete event
        stop = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3);
        while (records.size() < numEventsExpected && System.currentTimeMillis() < stop) {
            Thread.sleep(100);
        }

        // Stop the replicator ...
        replicator.stop();

        // Verify each record is valid and that we found the one new DELETE record we expect ...
        Set<ObjectId> foundIds = new HashSet<>();
        records.forEach(record -> {
            VerifyRecord.isValid(record);
            Struct key = (Struct) record.key();
            ObjectId id = (ObjectId)(JSON.parse(key.getString("id")));
            foundIds.add(id);
            if (record.value() != null) {
                Struct value = (Struct) record.value();
                Operation op = Operation.forCode(value.getString("op"));
                assertThat(op).isEqualTo(Operation.DELETE);
            }
        });
        assertThat(records.size()).isEqualTo(2); // 1 delete and 1 tombstone

        // ------------------------------------------------------------------------------
        // START REPLICATOR TO PERFORM SNAPSHOT
        // ------------------------------------------------------------------------------
        // Update the configuration and don't use a collection filter ...
        useConfiguration(config);

        // Start the replicator again ...
        records = new LinkedList<>();
        replicator = new Replicator(context, replicaSet, records::add);
        thread = new Thread(replicator::run);
        thread.start();

        // Sleep for 2 seconds ...
        Thread.sleep(2000);

        // Stop the replicator ...
        replicator.stop();

        // Verify each record is valid and that we found the two records we expect ...
        foundNames.clear();
        records.forEach(record -> {
            VerifyRecord.isValid(record);
            Struct value = (Struct) record.value();
            String after = value.getString("after");
            Document afterDoc = Document.parse(after);
            foundNames.add(afterDoc.getString("name"));
            Operation op = Operation.forCode(value.getString("op"));
            assertThat(op).isEqualTo(Operation.READ);
        });

        // We should not have found any new records ...
        assertThat(records.size()).isEqualTo(1);
        Object[] allExpectedNames = { "Sally Hamm" };
        assertThat(foundNames).containsOnly(allExpectedNames);
    }

}
