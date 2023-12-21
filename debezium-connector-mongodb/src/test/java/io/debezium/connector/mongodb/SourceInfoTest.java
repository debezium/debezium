/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.data.VerifyRecord.assertConnectSchemasAreEqual;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * @author Randall Hauch
 */

public class SourceInfoTest {

    private static String REPLICA_SET_NAME = "myReplicaSet";
    private SourceInfo source;
    private Map<String, String> partition;
    private MongoDbTaskContext context;

    @Before
    public void beforeEach() {
        Configuration config = Configuration.create()
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "serverX")
                .build();
        context = new MongoDbTaskContext(config);
        source = new SourceInfo(new MongoDbConnectorConfig(config), context.getMongoTaskId());
    }

    @Test
    public void shouldHaveSchemaForSource() {
        Schema schema = source.schema();
        assertThat(schema.name()).isNotEmpty();
        assertThat(schema.version()).isNull();
        assertThat(schema.field(SourceInfo.SERVER_NAME_KEY).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.REPLICA_SET_NAME).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.DATABASE_NAME_KEY).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.COLLECTION).schema()).isEqualTo(Schema.STRING_SCHEMA);
        assertThat(schema.field(SourceInfo.TIMESTAMP_KEY).schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(schema.field(SourceInfo.ORDER).schema()).isEqualTo(Schema.INT32_SCHEMA);
        assertThat(schema.field(SourceInfo.OPERATION_ID).schema()).isEqualTo(Schema.OPTIONAL_INT64_SCHEMA);
        assertThat(schema.field(SourceInfo.SNAPSHOT_KEY).schema()).isEqualTo(AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA);
    }

    @Test
    public void shouldProducePartitionMap() {
        partition = source.partition(REPLICA_SET_NAME);
        assertThat(partition.get(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(partition.get(SourceInfo.SERVER_ID_KEY)).isEqualTo("serverX");
        assertThat(partition.get(SourceInfo.MULTI_TASK_GEN_KEY)).isNull();
        assertThat(partition.get(SourceInfo.TASK_ID_KEY)).isNull();
        assertThat(partition.size()).isEqualTo(2);
    }

    @Test
    public void shouldReturnSamePartitionMapForSameReplicaName() {
        partition = source.partition(REPLICA_SET_NAME);
        assertThat(partition).isSameAs(source.partition(REPLICA_SET_NAME));
    }

    @Test
    public void shouldSetAndReturnRecordedOffset() {
        final BsonDocument event = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));
        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);
        source.opLogEvent(REPLICA_SET_NAME, event);
        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(true);

        Map<String, ?> offset = source.lastOffset(REPLICA_SET_NAME);
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo(100);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(offset.get(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);

        // Create a new source info and set the offset ...
        Map<String, String> partition = source.partition(REPLICA_SET_NAME);
        source = new SourceInfo(new MongoDbConnectorConfig(
                Configuration.create()
                        .with(MongoDbConnectorConfig.LOGICAL_NAME, "serverX")
                        .build()),
                context.getMongoTaskId());
        source.setOffsetFor(partition, offset);

        offset = source.lastOffset(REPLICA_SET_NAME);
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo(100);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(offset.get(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);

        BsonTimestamp ts = source.lastOffsetTimestamp(REPLICA_SET_NAME);
        assertThat(ts.getTime()).isEqualTo(100);
        assertThat(ts.getInc()).isEqualTo(2);

        source.collectionEvent(REPLICA_SET_NAME, new CollectionId(REPLICA_SET_NAME, "dbA", "collectA"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(100_000);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(struct.getInt64(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("dbA");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("collectA");
        assertThat(struct.getString(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    @Test
    public void shouldReturnOffsetForUnusedReplicaName() {
        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);

        Map<String, ?> offset = source.lastOffset(REPLICA_SET_NAME);
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo(0);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo(0);
        assertThat(offset.get(SourceInfo.OPERATION_ID)).isNull();

        BsonTimestamp ts = source.lastOffsetTimestamp(REPLICA_SET_NAME);
        assertThat(ts.getTime()).isEqualTo(0);
        assertThat(ts.getInc()).isEqualTo(0);

        source.collectionEvent(REPLICA_SET_NAME, new CollectionId(REPLICA_SET_NAME, "dbA", "collectA"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(0);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo(0);
        assertThat(struct.getInt64(SourceInfo.OPERATION_ID)).isNull();
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("dbA");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("collectA");
        assertThat(struct.getString(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isNull();

        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaName() {
        final BsonDocument event = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));

        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);
        source.opLogEvent(REPLICA_SET_NAME, event);
        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(true);

        Map<String, ?> offset = source.lastOffset(REPLICA_SET_NAME);
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo(100);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(offset.get(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);

        BsonTimestamp ts = source.lastOffsetTimestamp(REPLICA_SET_NAME);
        assertThat(ts.getTime()).isEqualTo(100);
        assertThat(ts.getInc()).isEqualTo(2);

        source.collectionEvent(REPLICA_SET_NAME, new CollectionId(REPLICA_SET_NAME, "dbA", "collectA"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(100_000);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(struct.getInt64(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("dbA");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("collectA");
        assertThat(struct.getString(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    @Test
    public void shouldReturnOffsetForUnusedReplicaNameDuringInitialSync() {
        source.startInitialSync(REPLICA_SET_NAME);
        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);

        Map<String, ?> offset = source.lastOffset(REPLICA_SET_NAME);
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo(0);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo(0);
        assertThat(offset.get(SourceInfo.OPERATION_ID)).isNull();

        BsonTimestamp ts = source.lastOffsetTimestamp(REPLICA_SET_NAME);
        assertThat(ts.getTime()).isEqualTo(0);
        assertThat(ts.getInc()).isEqualTo(0);

        source.collectionEvent(REPLICA_SET_NAME, new CollectionId(REPLICA_SET_NAME, "dbA", "collectA"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(0);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo(0);
        assertThat(struct.getInt64(SourceInfo.OPERATION_ID)).isNull();
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("dbA");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("collectA");
        assertThat(struct.getString(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");

        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);
    }

    @Test
    public void shouldReturnRecordedOffsetForUsedReplicaNameDuringInitialSync() {
        source.startInitialSync(REPLICA_SET_NAME);

        BsonDocument event = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));

        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(false);
        source.opLogEvent(REPLICA_SET_NAME, event);
        assertThat(source.hasOffset(REPLICA_SET_NAME)).isEqualTo(true);

        Map<String, ?> offset = source.lastOffset(REPLICA_SET_NAME);
        assertThat(offset.get(SourceInfo.TIMESTAMP)).isEqualTo(100);
        assertThat(offset.get(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(offset.get(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);

        BsonTimestamp ts = source.lastOffsetTimestamp(REPLICA_SET_NAME);
        assertThat(ts.getTime()).isEqualTo(100);
        assertThat(ts.getInc()).isEqualTo(2);

        source.collectionEvent(REPLICA_SET_NAME, new CollectionId(REPLICA_SET_NAME, "dbA", "collectA"), 0L);
        Struct struct = source.struct();
        assertThat(struct.getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(100_000);
        assertThat(struct.getInt32(SourceInfo.ORDER)).isEqualTo(2);
        assertThat(struct.getInt64(SourceInfo.OPERATION_ID)).isEqualTo(1987654321L);
        assertThat(struct.getString(SourceInfo.DATABASE_NAME_KEY)).isEqualTo("dbA");
        assertThat(struct.getString(SourceInfo.COLLECTION)).isEqualTo("collectA");
        assertThat(struct.getString(SourceInfo.REPLICA_SET_NAME)).isEqualTo(REPLICA_SET_NAME);
        assertThat(struct.getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("serverX");
        assertThat(struct.getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
    }

    @Test
    public void versionIsPresent() {
        final BsonDocument event = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));
        source.opLogEvent("rs", event);
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        final BsonDocument event = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));
        source.opLogEvent("rs", event);
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void wallTimeIsPresent() {
        // Non-transactional event.
        final BsonDocument event = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("wall", new BsonDateTime(1987654321123L))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));
        source.opLogEvent("rs", event);
        assertThat(source.struct().getInt64(SourceInfo.WALL_TIME)).isEqualTo(1987654321123L);
        // Transactional event.
        final BsonDocument masterEvent = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("wall", new BsonDateTime(1987654321123L))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));
        final BsonDocument oplogEvent = new BsonDocument().append("ts", new BsonTimestamp(100, 2))
                .append("h", new BsonInt64(Long.valueOf(1987654321)))
                .append("ns", new BsonString("dbA.collectA"));
        source.opLogEvent("rs", oplogEvent, masterEvent, 0);
        assertThat(source.struct().getInt64(SourceInfo.WALL_TIME)).isEqualTo(1987654321123L);
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.mongo.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("rs", Schema.STRING_SCHEMA)
                .field("collection", Schema.STRING_SCHEMA)
                .field("ord", Schema.INT32_SCHEMA)
                .field("h", Schema.OPTIONAL_INT64_SCHEMA)
                .field("tord", Schema.OPTIONAL_INT64_SCHEMA)
                .field("stxnid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("lsid", Schema.OPTIONAL_STRING_SCHEMA)
                .field("txnNumber", Schema.OPTIONAL_INT64_SCHEMA)
                .field("wallTime", Schema.OPTIONAL_INT64_SCHEMA)
                .field("stripeAudit", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        assertConnectSchemasAreEqual(null, source.schema(), schema);
    }
}
