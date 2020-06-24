/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * A common abstract base class for the Mongodb connector integration testing.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMongoConnectorIT extends AbstractConnectorTest {

    protected Configuration config;
    protected MongoDbTaskContext context;
    protected LogInterceptor logInterceptor;

    @Before
    public void beforEach() {
        Testing.Debug.disable();
        Testing.Print.disable();
        stopConnector();
        initializeConnectorTestFramework();
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            if (context != null) {
                context.getConnectionContext().shutdown();
            }
        }
    }

    /**
     * Load test documents from the classpath.
     *
     * @param pathOnClasspath the path on the classpath to the file containing the documents to load
     * @return list of loaded documents; never null but may contain no entries.
     */
    protected List<Document> loadTestDocuments(String pathOnClasspath) {
        final List<Document> documents = new ArrayList<>();
        try (InputStream stream = Testing.Files.readResourceAsStream(pathOnClasspath)) {
            assertThat(stream).isNotNull();
            IoUtil.readLines(stream, line -> {
                Document document = Document.parse(line);
                assertThat(document.size()).isGreaterThan(0);
                documents.add(document);
            });
        }
        catch (IOException e) {
            fail("Unable to find or read file '" + pathOnClasspath + "': " + e.getMessage());
        }
        return documents;
    }

    /**
     * Drops the specified collection if it exists and inserts all documents into the empty collection.
     *
     * NOTE: This method will only drop the collection if the documents list is provided.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param documents the documents to be inserted, can be empty
     */
    protected void dropAndInsertDocuments(String dbName, String collectionName, Document... documents) {
        // Do nothing if no documents are provided
        if (documents.length == 0) {
            return;
        }

        primary().execute("store documents", mongo -> {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' document");

            MongoDatabase db = mongo.getDatabase(dbName);

            MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.drop();

            for (Document document : documents) {
                InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
                assertThat(document).isNotNull();
                assertThat(document.size()).isGreaterThan(0);
                collection.insertOne(document, options);
            }
        });
    }

    /**
     * Inserts all documents in the specified collection.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param documents the documents to be inserted, can be empty
     */
    protected void insertDocuments(String dbName, String collectionName, Document... documents) {
        // Do nothing if no documents are provided
        if (documents.length == 0) {
            return;
        }

        primary().execute("store documents", mongo -> {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' document");

            MongoDatabase db = mongo.getDatabase(dbName);

            MongoCollection<Document> collection = db.getCollection(collectionName);

            for (Document document : documents) {
                InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
                assertThat(document).isNotNull();
                assertThat(document.size()).isGreaterThan(0);
                collection.insertOne(document, options);
            }
        });
    }

    /**
     * Inserts all documents in the specified collection within a transaction.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param documents the documents to be inserted, can be empty
     */
    protected void insertDocumentsInTx(String dbName, String collectionName, Document... documents) {
        assertThat(TestHelper.transactionsSupported(primary(), dbName)).isTrue();

        primary().execute("store documents in tx", mongo -> {
            Testing.debug("Storing documents in '" + dbName + "." + collectionName + "'");
            // If the collection does not exist, be sure to create it
            final MongoDatabase db = mongo.getDatabase(dbName);
            if (!collectionExists(db, collectionName)) {
                db.createCollection(collectionName);
            }

            final MongoCollection<Document> collection = db.getCollection(collectionName);

            final ClientSession session = mongo.startSession();
            try {
                session.startTransaction();

                final InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
                for (Document document : documents) {
                    assertThat(document).isNotNull();
                    assertThat(document.size()).isGreaterThan(0);
                    collection.insertOne(session, document, insertOptions);
                }

                session.commitTransaction();
            }
            finally {
                session.close();
            }
        });
    }

    /**
     * Updates a document in a collection based on a specified filter.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param filter the document filter
     * @param document the document fields to be updated
     */
    protected void updateDocument(String dbName, String collectionName, Document filter, Document document) {
        primary().execute("update", mongo -> {
            Testing.debug("Updating document with filter '" + filter + "' in '" + dbName + "." + collectionName + "'");

            MongoDatabase db = mongo.getDatabase(dbName);
            MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.updateOne(filter, document);
        });
    }

    /**
     * Deletes a document in a collection based on a specified filter.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param filter the document filter
     */
    protected void deleteDocuments(String dbName, String collectionName, Document filter) {
        primary().execute("delete", mongo -> {
            MongoDatabase db = mongo.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            coll.deleteOne(filter);
        });
    }

    protected MongoPrimary primary() {
        ReplicaSet replicaSet = ReplicaSet.parse(context.getConnectionContext().hosts());
        return context.getConnectionContext().primaryFor(replicaSet, context.filters(), connectionErrorHandler(3));
    }

    protected BiConsumer<String, Throwable> connectionErrorHandler(int numErrorsBeforeFailing) {
        AtomicInteger attempts = new AtomicInteger();
        return (desc, error) -> {
            if (attempts.incrementAndGet() > numErrorsBeforeFailing) {
                fail("Unable to connect to primary after " + numErrorsBeforeFailing + " errors trying to " + desc + ": " + error);
            }
            logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
        };
    }

    private static boolean collectionExists(MongoDatabase database, String collectionName) {
        final MongoIterable<String> collections = database.listCollectionNames();
        final MongoCursor<String> cursor = collections.cursor();
        while (cursor.hasNext()) {
            if (collectionName.equalsIgnoreCase(cursor.next())) {
                return true;
            }
        }
        return false;
    }

    protected void storeDocuments(String dbName, String collectionName, String pathOnClasspath) {
        primary().execute("storing documents", mongo -> {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = mongo.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            storeDocuments(coll, pathOnClasspath);
        });
    }

    protected void storeDocuments(MongoCollection<Document> collection, String pathOnClasspath) {
        InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
        loadTestDocuments(pathOnClasspath).forEach(doc -> {
            assertThat(doc).isNotNull();
            assertThat(doc.size()).isGreaterThan(0);
            collection.insertOne(doc, insertOptions);
        });
    }
}
