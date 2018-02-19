/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;

import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.data.Json;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.SchemaNameAdjuster;

/**
 * A component that makes {@link SourceRecord}s for {@link CollectionId collections} and submits them to a consumer.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class RecordMakers {

    private static final ObjectSerializer jsonSerializer = JSONSerializers.getStrict();
    private static final Map<String, Operation> operationLiterals = new HashMap<>();
    static {
        operationLiterals.put("i", Operation.CREATE);
        operationLiterals.put("u", Operation.UPDATE);
        operationLiterals.put("d", Operation.DELETE);
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(logger);
    private final SourceInfo source;
    private final TopicSelector topicSelector;
    private final Map<CollectionId, RecordsForCollection> recordMakerByCollectionId = new HashMap<>();
    private final Function<Document, String> valueTransformer;
    private final BlockingConsumer<SourceRecord> recorder;
    private final boolean emitTombstonesOnDelete;

    /**
     * Create the record makers using the supplied components.
     *
     * @param source the connector's source information; may not be null
     * @param topicSelector the selector for topic names; may not be null
     * @param recorder the potentially blocking consumer function to be called for each generated record; may not be null
     */
    public RecordMakers(SourceInfo source, TopicSelector topicSelector, BlockingConsumer<SourceRecord> recorder, boolean emitTombstonesOnDelete) {
        this.source = source;
        this.topicSelector = topicSelector;
        JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", ""); // most compact JSON
        this.valueTransformer = (doc) -> doc.toJson(writerSettings);
        this.recorder = recorder;
        this.emitTombstonesOnDelete = emitTombstonesOnDelete;
    }

    /**
     * Obtain the record maker for the given table, using the specified columns and sending records to the given consumer.
     *
     * @param collectionId the identifier of the collection for which records are to be produced; may not be null
     * @return the table-specific record maker; may be null if the table is not included in the connector
     */
    public RecordsForCollection forCollection(CollectionId collectionId) {
        return recordMakerByCollectionId.computeIfAbsent(collectionId, id -> {
            String topicName = topicSelector.getTopic(collectionId);
            return new RecordsForCollection(collectionId, source, topicName, schemaNameAdjuster, valueTransformer, recorder, emitTombstonesOnDelete);
        });
    }

    /**
     * A record producer for a given collection.
     */
    public static final class RecordsForCollection {
        private final CollectionId collectionId;
        private final String replicaSetName;
        private final SourceInfo source;
        private final Map<String, ?> sourcePartition;
        private final String topicName;
        private final Schema keySchema;
        private final Schema valueSchema;
        private final Function<Document, String> valueTransformer;
        private final BlockingConsumer<SourceRecord> recorder;
        private final boolean emitTombstonesOnDelete;

        protected RecordsForCollection(CollectionId collectionId, SourceInfo source, String topicName, SchemaNameAdjuster adjuster,
                Function<Document, String> valueTransformer, BlockingConsumer<SourceRecord> recorder, boolean emitTombstonesOnDelete) {
            this.sourcePartition = source.partition(collectionId.replicaSetName());
            this.collectionId = collectionId;
            this.replicaSetName = this.collectionId.replicaSetName();
            this.source = source;
            this.topicName = topicName;
            this.keySchema = SchemaBuilder.struct()
                                          .name(adjuster.adjust(topicName + ".Key"))
                                          .field("id", Schema.STRING_SCHEMA)
                                          .build();
            this.valueSchema = SchemaBuilder.struct()
                                            .name(adjuster.adjust(topicName + ".Envelope"))
                                            .field(FieldName.AFTER, Json.builder().optional().build())
                                            .field("patch", Json.builder().optional().build())
                                            .field(FieldName.SOURCE, source.schema())
                                            .field(FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                                            .field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                                            .build();
            JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", ""); // most compact JSON
            this.valueTransformer = (doc) -> doc.toJson(writerSettings, MongoClient.getDefaultCodecRegistry().get(Document.class));
            this.recorder = recorder;
            this.emitTombstonesOnDelete = emitTombstonesOnDelete;
        }

        /**
         * Get the identifier of the collection to which this producer applies.
         *
         * @return the collection ID; never null
         */
        public CollectionId collectionId() {
            return collectionId;
        }

        /**
         * Generate and record one or more source records to describe the given object.
         *
         * @param id the identifier of the collection in which the document exists; may not be null
         * @param object the document; may not be null
         * @param timestamp the timestamp at which this operation is occurring
         * @return the number of source records that were generated; will be 0 or more
         * @throws InterruptedException if the calling thread was interrupted while waiting to submit a record to
         *             the blocking consumer
         */
        public int recordObject(CollectionId id, Document object, long timestamp) throws InterruptedException {
            final Struct sourceValue = source.lastOffsetStruct(replicaSetName, id);
            final Map<String, ?> offset = source.lastOffset(replicaSetName);
            String objId = idObjToJson(object);
            assert objId != null;
            return createRecords(sourceValue, offset, Operation.READ, objId, object, timestamp);
        }
        /**
         * Generate and record one or more source records to describe the given event.
         *
         * @param oplogEvent the event; may not be null
         * @param timestamp the timestamp at which this operation is occurring
         * @return the number of source records that were generated; will be 0 or more
         * @throws InterruptedException if the calling thread was interrupted while waiting to submit a record to
         *             the blocking consumer
         */
        public int recordEvent(Document oplogEvent, long timestamp) throws InterruptedException {
            final Struct sourceValue = source.offsetStructForEvent(replicaSetName, oplogEvent);
            final Map<String, ?> offset = source.lastOffset(replicaSetName);
            Document patchObj = oplogEvent.get("o", Document.class);
            // Updates have an 'o2' field, since the updated object in 'o' might not have the ObjectID ...
            Object o2 = oplogEvent.get("o2");
            String objId = o2 != null ? idObjToJson(o2) : idObjToJson(patchObj);
            assert objId != null;
            Operation operation = operationLiterals.get(oplogEvent.getString("op"));
            return createRecords(sourceValue, offset, operation, objId, patchObj, timestamp);
        }

        protected int createRecords(Struct source, Map<String, ?> offset, Operation operation, String objId, Document objectValue,
                                    long timestamp)
                throws InterruptedException {
            Integer partition = null;
            Struct key = keyFor(objId);
            Struct value = new Struct(valueSchema);
            switch (operation) {
                case READ:
                case CREATE:
                    // The object is the new document ...
                    String jsonStr = valueTransformer.apply(objectValue);
                    value.put(FieldName.AFTER, jsonStr);
                    break;
                case UPDATE:
                    // The object is the idempotent patch document ...
                    String patchStr = valueTransformer.apply(objectValue);
                    value.put("patch", patchStr);
                    break;
                case DELETE:
                    // The delete event has nothing of any use, other than the _id which we already have in our key.
                    // So put nothing in the 'after' or 'patch' fields ...
                    break;
            }
            value.put(FieldName.SOURCE, source);
            value.put(FieldName.OPERATION, operation.code());
            value.put(FieldName.TIMESTAMP, timestamp);
            SourceRecord record = new SourceRecord(sourcePartition, offset, topicName, partition, keySchema, key, valueSchema, value);
            recorder.accept(record);

            if (operation == Operation.DELETE && emitTombstonesOnDelete) {
                // Also generate a tombstone event ...
                record = new SourceRecord(sourcePartition, offset, topicName, partition, keySchema, key, null, null);
                recorder.accept(record);
                return 2;
            }
            return 1;
        }

        protected String idObjToJson(Object idObj) {
            if (idObj == null) {
                return null;
            }
            if (!(idObj instanceof Document)) {
                return jsonSerializer.serialize(idObj);
            }
            return jsonSerializer.serialize(
                    ((Document)idObj).get(DBCollection.ID_FIELD_NAME)
            );
        }

        protected Struct keyFor(String objId) {
            return new Struct(keySchema).put("id", objId);
        }
    }

    /**
     * Clear all of the cached record makers. This should be done when the logs are rotated, since in that a different table
     * numbering scheme will be used by all subsequent TABLE_MAP binlog events.
     */
    public void clear() {
        logger.debug("Clearing table converters");
        recordMakerByCollectionId.clear();
    }
}
