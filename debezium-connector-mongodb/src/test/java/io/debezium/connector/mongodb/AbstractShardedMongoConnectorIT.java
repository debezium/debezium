/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
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

import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseVersionResolver;
import io.debezium.connector.mongodb.junit.MongoDbPlatform;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.MongoDbShardedCluster;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.util.Testing;

public class AbstractShardedMongoConnectorIT extends AbstractConnectorTest {
    protected static final String DEFAULT_DATABASE = "dbit";

    protected static final String DEFAULT_COLLECTION = "items";
    protected static final String DEFAULT_SHARDING_KEY = "_id";

    protected static MongoDbShardedCluster mongo;

    protected static MongoClient connect() {
        return connect(mongo);
    }

    protected static MongoClient connect(MongoDbDeployment mongo) {
        return MongoClients.create(mongo.getConnectionString());
    }

    @BeforeClass
    public static void beforeAll() {
        Assume.assumeTrue(MongoDbDatabaseVersionResolver.getPlatform().equals(MongoDbPlatform.MONGODB_DOCKER));
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.mongoDbShardedCluster();
        mongo.start();
    }

    @AfterClass
    public static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            mongo.stop();
        }
    }

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();

        var database = shardedDatabase();
        cleanDatabase(mongo, database);
        mongo.enableSharding(database);

        shardedCollections().forEach((collection, key) -> {
            mongo.shardCollection(database, collection, key);
        });
    }

    @After
    public void afterEach() {
        stopConnector();
    }

    protected String shardedDatabase() {
        return DEFAULT_DATABASE;
    }

    protected Map<String, String> shardedCollections() {
        return Map.of(DEFAULT_COLLECTION, DEFAULT_SHARDING_KEY);
    }

    protected String shardedCollection() {
        return shardedCollections().keySet().stream().findFirst().orElseThrow();
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

    protected void verifyNotFromInitialSnapshot(SourceRecord record) {
        assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isFalse();
        Struct value = (Struct) record.value();
        assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    protected void verifyFromInitialSnapshot(SourceRecord record, AtomicBoolean foundLast) {
        if (record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)) {
            assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isTrue();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        }
        else {
            // Only the last record in the initial sync should be marked as not being part of the initial sync ...
            assertThat(foundLast.getAndSet(true)).isFalse();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
        }
    }

    protected void verifyOperation(SourceRecord record, Envelope.Operation expected) {
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(expected.code());
    }
}
