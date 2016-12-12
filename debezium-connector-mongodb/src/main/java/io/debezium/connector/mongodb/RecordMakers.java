/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.datapipeline.base.converter.ConnectorDataGenerator;
import com.datapipeline.base.converter.ConnectorType;
import com.datapipeline.base.converter.Converters;
import com.datapipeline.base.exceptions.NeedOpsException;
import com.datapipeline.base.mongodb.MongodbSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.data.Json;
import io.debezium.function.BlockingConsumer;
import io.debezium.util.AvroValidator;

/**
 * A component that makes {@link SourceRecord}s for {@link CollectionId collections} and submits them to a consumer.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public class RecordMakers {

    private static final Map<String, Operation> operationLiterals = new HashMap<>();
    static {
        operationLiterals.put("i", Operation.CREATE);
        operationLiterals.put("u", Operation.UPDATE);
        operationLiterals.put("d", Operation.DELETE);
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AvroValidator schemaNameValidator = AvroValidator.create(logger);
    private final SourceInfo source;
    private final TopicSelector topicSelector;
    private final Map<CollectionId, RecordsForCollection> recordMakerByCollectionId = new HashMap<>();
    private final Function<Document, String> valueTransformer;
    private final BlockingConsumer<SourceRecord> recorder;

    private final MongodbSchemaManager schemaManager;

    /**
     * Create the record makers using the supplied components.
     * 
     * @param source the connector's source information; may not be null
     * @param topicSelector the selector for topic names; may not be null
     * @param recorder the potentially blocking consumer function to be called for each generated record; may not be null
     */
    public RecordMakers(SourceInfo source, TopicSelector topicSelector, BlockingConsumer<SourceRecord> recorder, MongodbSchemaManager schemaManager) {
        this.source = source;
        this.topicSelector = topicSelector;
        JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", ""); // most compact JSON
        this.valueTransformer = (doc) -> doc.toJson(writerSettings);
        this.recorder = recorder;
        this.schemaManager = schemaManager;
    }

    /**
     * Obtain the record maker for the given table, using the specified columns and sending records to the given consumer.
     * 
     * @param collectionId the identifier of the collection for which records are to be produced; may not be null
     * @return the table-specific record maker; may be null if the table is not included in the connector
     */
    public RecordsForCollection forCollection(CollectionId collectionId) throws NeedOpsException {
        if(schemaManager == null){
            throw new NeedOpsException("Schema manager is null, can't build record for collection : "
                    + collectionId.namespace() + " . Please check if collection schema is correctly set. ");
        }

        List<MongodbSchema> mongodbSchemas = schemaManager.getMongodbSchemasForCollection(collectionId.dbName(),collectionId.name());
        if(mongodbSchemas == null){
            throw new NeedOpsException("Schema is null, can't build record for collection : "
                    + collectionId.namespace() + " . Please check if collection schema is correctly set. ");
        }
        return recordMakerByCollectionId.computeIfAbsent(collectionId, id -> {
            String topicName = topicSelector.getTopic(collectionId);
            return new RecordsForCollection(collectionId, source, topicName, schemaNameValidator, valueTransformer, recorder,mongodbSchemas);
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
        private final Schema afterValueSchema;
//        private final Schema valueSchema;
        private final AvroValidator validator;
        private final Function<Document, String> valueTransformer;
        private final BlockingConsumer<SourceRecord> recorder;

        private final ConnectorDataGenerator dataGenerator;
        private final List<MongodbSchema> mongodbSchemas;

        protected RecordsForCollection(CollectionId collectionId, SourceInfo source, String topicName, AvroValidator validator,
                Function<Document, String> valueTransformer, BlockingConsumer<SourceRecord> recorder, List<MongodbSchema> mongodbSchemas) {
            this.sourcePartition = source.partition(collectionId.replicaSetName());
            this.collectionId = collectionId;
            this.replicaSetName = this.collectionId.replicaSetName();
            this.source = source;
            this.topicName = topicName;
            dataGenerator = new ConnectorDataGenerator();
            this.keySchema = dataGenerator.buildKeySchema(validator.validate(topicName + ".Key"), ConnectorType.MONGODB);
            this.validator = validator;
            this.afterValueSchema = dataGenerator.buildValueSchema(null, mongodbSchemas);
            this.valueSchema = SchemaBuilder.struct()
                                            .name(validator.validate(topicName + ".Envelope"))
                                            .field(FieldName.AFTER, afterValueSchema)
                                            .field("patch", Json.builder().optional().build())
                                            .field(FieldName.SOURCE, source.schema())
                                            .field(FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                                            .field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                                            .build();
            this.mongodbSchemas = mongodbSchemas;

            JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", ""); // most compact JSON
            this.valueTransformer = (doc) -> doc.toJson(writerSettings);
            this.recorder = recorder;
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
            String objId = objectIdLiteralFrom(object);
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
            String objId = o2 != null ? objectIdLiteral(o2) : objectIdLiteralFrom(patchObj);
            assert objId != null;
            Operation operation = operationLiterals.get(oplogEvent.getString("op"));
            return createRecords(sourceValue, offset, operation, objId, patchObj, timestamp);
        }

        protected int createRecords(Struct source, Map<String, ?> offset, Operation operation, String objId, Document objectValue,
                                    long timestamp)
                throws InterruptedException {
            Integer partition = null;
            Struct key = dataGenerator.buildKeyStruct(keySchema,ConnectorType.MONGODB,objId);

            List<MongoDbColumnData> datas = new LinkedList<>();
            for(MongodbSchema schema: mongodbSchemas){
                if(objectValue.get(schema.getName())!=null){
                    Object value = Converters.getConverter(schema.getDataType()).convert(objectValue.get(schema.getName()).toString());
                    MongoDbColumnData data = new MongoDbColumnData(value,schema);
                    datas.add(data);
                }
            }

            if(objectValue.size() > mongodbSchemas.size()){
                logger.error("Source data has more schema then defined. " + objectValue.toString());
            }

            Struct value = new Struct(valueSchema);
            Struct valueStruct = dataGenerator.buildValueStruct(afterValueSchema, datas);
            switch (operation) {
                case READ:
                case CREATE:
                    // The object is the new document ...
                    String jsonStr = valueTransformer.apply(objectValue);
                    value.put(FieldName.AFTER, valueStruct);
                    break;
                case UPDATE:
                    // The object is the idempotent patch document ...
                    String patchStr = valueTransformer.apply(objectValue);
                    value.put("patch", valueStruct);
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

            if (operation == Operation.DELETE) {
                // Also generate a tombstone event ...
                record = new SourceRecord(sourcePartition, offset, topicName, partition, keySchema, key, null, null);
                recorder.accept(record);
                return 2;
            }
            return 1;
        }

        protected String objectIdLiteralFrom(Document obj) {
            if (obj == null) {
                return null;
            }
            Object id = obj.get("_id");
            return objectIdLiteral(id);
        }

        protected String objectIdLiteral(Object id) {
            if (id == null) {
                return null;
            }
            if (id instanceof ObjectId) {
                return ((ObjectId) id).toHexString();
            }
            if (id instanceof String) {
                return (String) id;
            }
            if (id instanceof Document) {
                return valueTransformer.apply((Document) id);
            }
            return id.toString();
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
