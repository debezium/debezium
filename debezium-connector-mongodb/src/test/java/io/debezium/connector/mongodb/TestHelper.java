/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.fest.assertions.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoDatabase;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;

/**
 * A common test configuration options
 *
 * @author Jiri Pechanec
 *
 */
public class TestHelper {
    protected final static Logger logger = LoggerFactory.getLogger(TestHelper.class);

    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Configuration getConfiguration() {
        final Builder cfgBuilder = Configuration.fromSystemProperties("connector.").edit()
                .withDefault(MongoDbConnectorConfig.HOSTS, "rs0/localhost:27017")
                .withDefault(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, false)
                .withDefault(MongoDbConnectorConfig.LOGICAL_NAME, "mongo1");
        if (isOplogCaptureMode()) {
            cfgBuilder.withDefault(MongoDbConnectorConfig.CAPTURE_MODE, CaptureMode.OPLOG);
        }
        return cfgBuilder.build();
    }

    public static void cleanDatabase(MongoPrimary primary, String dbName) {
        primary.execute("clean-db", mongo -> {
            MongoDatabase db1 = mongo.getDatabase(dbName);
            db1.listCollectionNames().forEach((Consumer<String>) ((String x) -> {
                logger.info("Removing collection '{}' from database '{}'", x, dbName);
                db1.getCollection(x).drop();
            }));
        });
    }

    public static Document databaseInformation(MongoPrimary primary, String dbName) {
        final AtomicReference<Document> ret = new AtomicReference<>();
        primary.execute("clean-db", mongo -> {
            MongoDatabase db1 = mongo.getDatabase(dbName);
            final BsonDocument command = new BsonDocument();
            command.put("buildinfo", new BsonString(""));
            ret.set(db1.runCommand(command));
        });
        return ret.get();
    }

    public static boolean transactionsSupported(MongoPrimary primary, String dbName) {
        final Document serverInfo = databaseInformation(primary, dbName);
        @SuppressWarnings("unchecked")
        final List<Integer> version = (List<Integer>) serverInfo.get("versionArray");
        return version.get(0) >= 4;
    }

    public static boolean decimal128Supported(MongoPrimary primary, String dbName) {
        final Document serverInfo = databaseInformation(primary, dbName);
        @SuppressWarnings("unchecked")
        final List<Integer> version = (List<Integer>) serverInfo.get("versionArray");
        return (version.get(0) >= 4) || (version.get(0) == 3 && version.get(1) >= 4);
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

    public static final boolean isOplogCaptureMode() {
        return "oplog".equals(captureMode());
    }

    public static void assertChangeStreamUpdate(ObjectId oid, Struct value, String after, List<String> removedFields,
                                                String updatedFields) {
        Assertions.assertThat(value.getString("after")).isEqualTo(after.replace("<OID>", oid.toHexString()));
        Assertions.assertThat(value.getStruct("updateDescription").getString("updatedFields")).isEqualTo(updatedFields);
        Assertions.assertThat(value.getStruct("updateDescription").getArray("removedFields")).isEqualTo(removedFields);
    }

    public static void assertChangeStreamUpdateAsDocs(ObjectId oid, Struct value, String after,
                                                      List<String> removedFields, String updatedFields) {
        Document expectedAfter = TestHelper.getDocumentWithoutLanguageVersion(after.replace("<OID>", oid.toHexString()));
        Document actualAfter = TestHelper
                .getDocumentWithoutLanguageVersion(value.getString("after"));
        Assertions.assertThat(actualAfter).isEqualTo(expectedAfter);
        final String actualUpdatedFields = value.getStruct("updateDescription").getString("updatedFields");
        if (actualUpdatedFields != null) {
            Assertions.assertThat(updatedFields).isNotNull();
            try {
                Assertions.assertThat((Object) mapper.readTree(actualUpdatedFields)).isEqualTo(mapper.readTree(updatedFields));
            }
            catch (JsonProcessingException e) {
                fail("Failed to parse JSON <" + actualUpdatedFields + "> or <" + updatedFields + ">");
            }
        }
        else {
            Assertions.assertThat(updatedFields).isNull();
        }
        final List<Object> actualRemovedFields = value.getStruct("updateDescription").getArray("removedFields");
        if (actualRemovedFields != null) {
            Assertions.assertThat(removedFields).isNotNull();
            Assertions.assertThat(actualRemovedFields.containsAll(removedFields) && removedFields.containsAll(actualRemovedFields));
        }
        else {
            Assertions.assertThat(removedFields).isNull();
        }
    }
}