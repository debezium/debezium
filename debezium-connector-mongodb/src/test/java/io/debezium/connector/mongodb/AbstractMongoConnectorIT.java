/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.util.Collect;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * A common abstract base class for the Mongodb connector integration testing.
 *
 * @author Chris Cranford
 */
public abstract class AbstractMongoConnectorIT extends AbstractConnectorTest {

    // the one and only task we start in the test suite
    private static final int TASK_ID = 0;

    protected static MongoDbReplicaSet mongo;

    protected Configuration config;
    protected MongoDbTaskContext context;
    protected LogInterceptor logInterceptor;

    @Before
    public void beforeEach() {
        Debug.disable();
        Print.disable();
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

    @BeforeClass
    public static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.mongoDbReplicaSet();
        mongo.start();
    }

    @AfterClass
    public static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            mongo.stop();
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
        try (InputStream stream = Files.readResourceAsStream(pathOnClasspath)) {
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

        try (var client = TestHelper.connect(mongo)) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' document");

            MongoDatabase db = client.getDatabase(dbName);

            MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.drop();

            for (Document document : documents) {
                InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
                assertThat(document).isNotNull();
                assertThat(document.size()).isGreaterThan(0);
                collection.insertOne(document, options);
            }
        }
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

        try (var client = TestHelper.connect(mongo)) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' document");

            MongoDatabase db = client.getDatabase(dbName);

            MongoCollection<Document> collection = db.getCollection(collectionName);

            for (Document document : documents) {
                InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
                assertThat(document).isNotNull();
                assertThat(document.size()).isGreaterThan(0);
                collection.insertOne(document, options);
            }
        }
    }

    /**
     * Inserts all documents in the specified collection within a transaction.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param documents the documents to be inserted, can be empty
     */
    protected void insertDocumentsInTx(String dbName, String collectionName, Document... documents) {
        assertThat(TestHelper.transactionsSupported()).isTrue();

        try (var client = TestHelper.connect(mongo)) {
            Testing.debug("Storing documents in '" + dbName + "." + collectionName + "'");
            // If the collection does not exist, be sure to create it
            final MongoDatabase db = client.getDatabase(dbName);
            if (!collectionExists(db, collectionName)) {
                db.createCollection(collectionName);
            }

            final MongoCollection<Document> collection = db.getCollection(collectionName);

            final ClientSession session = client.startSession();
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
        }
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
        try (var client = TestHelper.connect(mongo)) {
            Testing.debug("Updating document with filter '" + filter + "' in '" + dbName + "." + collectionName + "'");

            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.updateOne(filter, document);
        }
    }

    /**
     * Updates documents in a collection based on a specified filter within a transaction.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param filter the document filter
     * @param document the document fields to be updated
     */
    protected void updateDocumentsInTx(String dbName, String collectionName, Document filter, Document document) {
        assertThat(TestHelper.transactionsSupported()).isTrue();

        try (var client = connect()) {
            Testing.debug("Updating document with filter '" + filter + "' in '" + dbName + "." + collectionName + "'");

            final MongoDatabase db = client.getDatabase(dbName);
            final MongoCollection<Document> collection = db.getCollection(collectionName);

            final ClientSession session = client.startSession();
            try {
                session.startTransaction();

                collection.updateMany(filter, document);

                session.commitTransaction();
            }
            finally {
                session.close();
            }
        }
    }

    /**
     * Deletes a document in a collection based on a specified filter.
     *
     * @param dbName the database name
     * @param collectionName the collection name
     * @param filter the document filter
     */
    protected void deleteDocuments(String dbName, String collectionName, Document filter) {
        try (var client = connect()) {
            MongoDatabase db = client.getDatabase(dbName);
            MongoCollection<Document> coll = db.getCollection(collectionName);
            coll.deleteOne(filter);
        }
    }

    protected MongoClient connect() {
        return MongoClients.create(mongo.getConnectionString());
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
        try (var client = connect()) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            storeDocuments(coll, pathOnClasspath);
        }
    }

    protected void storeDocuments(MongoCollection<Document> collection, String pathOnClasspath) {
        InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
        loadTestDocuments(pathOnClasspath).forEach(doc -> {
            assertThat(doc).isNotNull();
            assertThat(doc.size()).isGreaterThan(0);
            collection.insertOne(doc, insertOptions);
        });
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server) {
        return getSnapshotMetricsObjectName(connector, server, TASK_ID);
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server, int taskId) {
        return getMetricsObjectNameWithTags(connector,
                Collect.linkMapOf("context", "snapshot", "server", server, "task", String.valueOf(taskId)));
    }

    public static void waitForSnapshotToBeCompleted(String connector, String server) {
        waitForSnapshotToBeCompleted(getSnapshotMetricsObjectName(connector, server));
    }

    public static void waitForSnapshotToBeCompleted(String connector, String server, int taskId) {
        waitForSnapshotToBeCompleted(getSnapshotMetricsObjectName(connector, server, taskId));
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server) {
        return getStreamingMetricsObjectName(connector, server, TASK_ID);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, int taskId) {
        return getMetricsObjectNameWithTags(connector,
                Collect.linkMapOf("context", getStreamingNamespace(), "server", server, "task", String.valueOf(taskId)));
    }

    public static void waitForStreamingRunning(String connector, String server) {
        waitForStreamingRunning(getStreamingMetricsObjectName(connector, server));
    }

    public static void waitForStreamingRunning(String connector, String server, int taskId) {
        waitForStreamingRunning(getStreamingMetricsObjectName(connector, server, taskId));
    }

    private static void waitForSnapshotToBeCompleted(ObjectName objectName) {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> (boolean) mbeanServer.getAttribute(objectName, "SnapshotCompleted"));
    }

    private static void waitForStreamingRunning(ObjectName objectName) {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> (boolean) mbeanServer.getAttribute(objectName, "Connected"));
    }

    private static ObjectName getMetricsObjectNameWithTags(String connector, Map<String, String> tags) {
        try {
            return new ObjectName("debezium." + connector + ":type=connector-metrics,"
                    + tags.entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining(",")));
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }
}
