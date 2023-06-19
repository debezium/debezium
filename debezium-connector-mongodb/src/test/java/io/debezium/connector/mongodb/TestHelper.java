/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.testing.testcontainers.MongoDbDeployment;

/**
 * A common test configuration options
 *
 * @author Jiri Pechanec
 *
 */
public class TestHelper {
    protected final static Logger logger = LoggerFactory.getLogger(TestHelper.class);

    public static final List<Integer> MONGO_VERSION = getMongoVersion();
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final ObjectMapper mapper = new ObjectMapper();

    private static List<Integer> getMongoVersion() {
        var prop = System.getProperty("version.mongo.server", "6.0");
        var parts = prop.split("\\.");

        return Stream.concat(Arrays.stream(parts), Stream.of("0", "0", "0"))
                .limit(3)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    public static String connectionString(MongoDbDeployment mongo) {
        return ConnectionStrings.appendParameter(mongo.getConnectionString(), "readPreference", "secondaryPreferred");
    }

    public static Configuration getConfiguration() {
        return getConfiguration("mongodb://dummy:27017");
    }

    public static Configuration getConfiguration(MongoDbDeployment mongo) {
        var cs = connectionString(mongo);
        return getConfiguration(cs);
    }

    public static Configuration getConfiguration(String connectionString) {
        final Builder cfgBuilder = Configuration.fromSystemProperties("connector.").edit()
                .withDefault(MongoDbConnectorConfig.CONNECTION_STRING, connectionString)
                .withDefault(CommonConnectorConfig.TOPIC_PREFIX, "mongo1");
        return cfgBuilder.build();
    }

    public static MongoDbConnection.ErrorHandler connectionErrorHandler(int numErrorsBeforeFailing) {
        AtomicInteger attempts = new AtomicInteger();
        return (desc, error) -> {
            if (attempts.incrementAndGet() > numErrorsBeforeFailing) {
                fail("Unable to connect to primary after " + numErrorsBeforeFailing + " errors trying to " + desc + ": " + error);
            }
            logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
        };
    }

    public static MongoClient connect(MongoDbDeployment mongo) {
        return MongoClients.create(mongo.getConnectionString());
    }

    public static void cleanDatabase(MongoDbDeployment mongo, String dbName) {
        try (var client = connect(mongo)) {
            MongoDatabase db1 = client.getDatabase(dbName);
            db1.listCollectionNames().forEach((String x) -> {
                logger.info("Removing collection '{}' from database '{}'", x, dbName);
                db1.getCollection(x).drop();
            });
        }
    }

    public static boolean transactionsSupported() {
        return MONGO_VERSION.get(0) >= 4;
    }

    public static boolean decimal128Supported() {
        return (MONGO_VERSION.get(0) >= 4) || (MONGO_VERSION.get(0) == 3 && MONGO_VERSION.get(1) >= 4);
    }

    public static String lines(String... lines) {
        final StringBuilder sb = new StringBuilder();
        Arrays.stream(lines).forEach(line -> sb.append(line).append(System.lineSeparator()));
        return sb.toString();
    }

    public static Document getDocumentWithoutLanguageVersion(String jsonString) {
        // MongoDB 3.6+ added the internal field '$v' which generally is accompanied by the value '1'.
        // For compatibility against older MongoDB versions in tests, use this to remove such fields.
        final Document document = Document.parse(jsonString);
        document.remove("$v");
        return document;
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    public static String captureMode() {
        return System.getProperty(TEST_PROPERTY_PREFIX + "capture.mode", "changestreams");
    }

    public static void assertChangeStreamUpdate(ObjectId oid, Struct value, String after, List<String> removedFields,
                                                String updatedFields) {
        assertThat(value.getString("after")).isEqualTo(after.replace("<OID>", oid.toHexString()));
        assertThat(value.getStruct("updateDescription").getString("updatedFields")).isEqualTo(updatedFields);
        assertThat(value.getStruct("updateDescription").getArray("removedFields")).isEqualTo(removedFields);
    }

    public static void assertChangeStreamUpdateAsDocs(ObjectId oid, Struct value, String after,
                                                      List<String> removedFields, String updatedFields) {
        Document expectedAfter = TestHelper.getDocumentWithoutLanguageVersion(after.replace("<OID>", oid.toHexString()));
        Document actualAfter = TestHelper
                .getDocumentWithoutLanguageVersion(value.getString("after"));
        assertThat(actualAfter).isEqualTo(expectedAfter);
        final String actualUpdatedFields = value.getStruct("updateDescription").getString("updatedFields");
        if (actualUpdatedFields != null) {
            assertThat(updatedFields).isNotNull();
            try {
                assertThat((Object) mapper.readTree(actualUpdatedFields)).isEqualTo(mapper.readTree(updatedFields));
            }
            catch (JsonProcessingException e) {
                fail("Failed to parse JSON <" + actualUpdatedFields + "> or <" + updatedFields + ">");
            }
        }
        else {
            assertThat(updatedFields).isNull();
        }
        final List<Object> actualRemovedFields = value.getStruct("updateDescription").getArray("removedFields");
        if (actualRemovedFields != null) {
            assertThat(removedFields).isNotNull();
            assertThat(actualRemovedFields.containsAll(removedFields) && removedFields.containsAll(actualRemovedFields));
        }
        else {
            assertThat(removedFields).isNull();
        }
    }

    public static ReplicaSet replicaSet(MongoDbDeployment mongo) {
        var cs = connectionString(mongo);
        return new ReplicaSet(cs);
    }
}
