/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.UpdateOperators;

import static org.fest.assertions.Assertions.assertThat;

import java.util.function.Consumer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.UpdateOptions;

import io.debezium.connector.mongodb.TestHelper;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.data.VerifyRecord;

/**
 * Integration test for {@link ExtractNewDocumentState}.
 * <p>
 * This subset of tests cover the Field Update Operator as in the official documentation
 * {@see https://docs.mongodb.com/v3.6/reference/operator/update-field/#field-update-operators}
 *
 * @author Renato Mefi
 */
public class ExtractNewDocumentStateUpdateFieldOperatorTestIT extends AbstractExtractNewDocumentStateUpdateOperatorsTestIT {

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/inc/#up._S_inc">MongoDB operator update $inc</a>
     */
    @Test
    public void shouldTransformOperationInc() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$inc': {'dataInt': 123, 'nested.dataInt': -23}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(246);
        if (TestHelper.isOplogCaptureMode()) {
            VerifyRecord.assertConnectSchemasAreEqual("nested.dataInt", valueSchema.field("nested.dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
            assertThat(transformedUpdateValue.get("nested.dataInt")).isEqualTo(100);
        }
        else {
            VerifyRecord.assertConnectSchemasAreEqual("nested.dataInt",
                    valueSchema.field("nested").schema().field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
            assertThat(transformedUpdateValue.getStruct("nested").get("dataInt")).isEqualTo(100);
        }
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/min/#up._S_min">MongoDB operator update $min</a>
     */
    @Test
    public void shouldTransformOperationMin() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$min': {'dataInt': 122, 'nested.dataInt': 124}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(122);
        if (TestHelper.isOplogCaptureMode()) {
            // Since 124 > 123 we should expect "nested.dataInt" to not be present
            assertThat(valueSchema.field("nested.dataInt")).isNull();
        }
        else {
            assertThat(transformedUpdateValue.getStruct("nested").get("dataInt")).isEqualTo(123);
        }
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/max/#up._S_max">MongoDB operator update $max</a>
     */
    @Test
    public void shouldTransformOperationMax() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$max': {'dataInt': 122, 'nested.dataInt': 124}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        if (TestHelper.isOplogCaptureMode()) {
            // Since 122 < 123 we should expect "dataInt" to not be present
            assertThat(valueSchema.field("dataInt")).isNull();
            VerifyRecord.assertConnectSchemasAreEqual("nested.dataInt", valueSchema.field("nested.dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
            assertThat(transformedUpdateValue.get("nested.dataInt")).isEqualTo(124);
        }
        else {
            assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(123);
            assertThat(transformedUpdateValue.getStruct("nested").get("dataInt")).isEqualTo(124);
        }
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/mul/#up._S_mul">MongoDB operator update $mul</a>
     */
    @Test
    public void shouldTransformOperationMul() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$mul': {'dataInt': 3, 'nested.dataInt': 2, 'nonExistentField': 123}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("nonExistentField", valueSchema.field("nonExistentField").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(369);
        assertThat(transformedUpdateValue.get("nonExistentField")).isEqualTo(0);
        if (TestHelper.isOplogCaptureMode()) {
            VerifyRecord.assertConnectSchemasAreEqual("nested.dataInt", valueSchema.field("nested.dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
            assertThat(transformedUpdateValue.get("nested.dataInt")).isEqualTo(246);
        }
        else {
            VerifyRecord.assertConnectSchemasAreEqual("nested.dataInt",
                    valueSchema.field("nested").schema().field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
            assertThat(transformedUpdateValue.getStruct("nested").get("dataInt")).isEqualTo(246);
        }
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/rename/#up._S_rename">MongoDB operator update $rename</a>
     */
    @Test
    public void shouldTransformOperationRename() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$rename': {'dataInt': 'dataIntNewName', 'nonExistentField': 'nonExistentFieldRenamed'}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataIntNewName", valueSchema.field("dataIntNewName").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataIntNewName")).isEqualTo(123);

        if (TestHelper.isOplogCaptureMode()) {
            // Ensure the rename causes the old field value to be set to null
            VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_STRING_SCHEMA);
            assertThat(transformedUpdateValue.get("dataInt")).isNull();
        }
        else {
            assertThat(valueSchema.field("dataInt")).isNull();
        }
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/set/#up._S_set">MongoDB operator update $set</a>
     * For more extensive tests for the $set operator please check:
     * {@link io.debezium.connector.mongodb.transforms.ExtractNewDocumentStateTest}
     * {@link io.debezium.connector.mongodb.transforms.ExtractNewDocumentStateTestIT}
     */
    @Test
    public void shouldTransformOperationSet() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$set': {'dataStr': 'Setting new value', 'newDataInt': 456}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataStr", valueSchema.field("dataStr").schema(), Schema.OPTIONAL_STRING_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("newDataInt", valueSchema.field("newDataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataStr")).isEqualTo("Setting new value");
        assertThat(transformedUpdateValue.get("newDataInt")).isEqualTo(456);
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/setOnInsert/#up._S_setOnInsert">MongoDB operator update $setOnInsert</a>
     */
    @Test
    public void shouldTransformOperationSetOnInsert() throws InterruptedException {
        Bson setOnInsert = Document.parse("{'$setOnInsert': {'onlySetIfInsertDataInt': 789}}");
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        Consumer<MongoClient> upsert = client -> client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                .updateOne(Document.parse("{'_id' : 2}"), setOnInsert, updateOptions);

        primary().execute("update", upsert);

        SourceRecord upsertRecord = consumeRecordsByTopic(1).recordsForTopic(this.topicName()).get(0);

        final SourceRecord transformedUpsert = transformation.apply(upsertRecord);
        final Struct transformedUpsertValue = (Struct) transformedUpsert.value();
        final Schema upsertValueSchema = transformedUpsert.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", upsertValueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("onlySetIfInsertDataInt", upsertValueSchema.field("onlySetIfInsertDataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpsertValue.get("id")).isEqualTo(2);
        assertThat(transformedUpsertValue.get("onlySetIfInsertDataInt")).isEqualTo(789);

        // Execute a new Upsert with the same ID to ensure the field "onlySetIfInsertDataInt" doesn't change its value
        Bson setOnInsertAndSet = Document.parse("{'$setOnInsert': {'onlySetIfInsertDataInt': 123}, '$set': {'newField': 456}}");
        Consumer<MongoClient> upsertAndUpdate = client -> client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                .updateOne(Document.parse("{'_id' : 2}"), setOnInsertAndSet, updateOptions);
        primary().execute("update", upsertAndUpdate);

        SourceRecord updateRecord = getUpdateRecord();
        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema updateValueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", updateValueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("newField", updateValueSchema.field("newField").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(2);
        assertThat(transformedUpdateValue.get("newField")).isEqualTo(456);
        if (TestHelper.isOplogCaptureMode()) {
            // Ensure on the second update the field is not set
            assertThat(updateValueSchema.field("onlySetIfInsertDataInt")).isNull();
        }
        else {
            assertThat(transformedUpdateValue.get("onlySetIfInsertDataInt")).isEqualTo(789);
        }
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/unset/#up._S_unset">MongoDB operator update $unset</a>
     * For more details on how the Unset is implemented please refer to:
     * @see <a href="https://github.com/debezium/debezium/pull/669">DBZ-612 Implementation</a>
     */
    @Test
    public void shouldTransformOperationUnset() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$unset': {'dataStr': '', 'nonExistentField': ''}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);

        if (TestHelper.isOplogCaptureMode()) {
            // Unset fields come as null value
            VerifyRecord.assertConnectSchemasAreEqual("dataStr", valueSchema.field("dataStr").schema(), Schema.OPTIONAL_STRING_SCHEMA);
            assertThat(transformedUpdateValue.get("dataStr")).isNull();
        }
        else {
            assertThat(valueSchema.field("dataStr")).isNull();
        }
        // Since the field "nonExistentField" doesn't exist ensure it's not present in the schema
        assertThat(valueSchema.field("nonExistentField")).isNull();
    }
}
