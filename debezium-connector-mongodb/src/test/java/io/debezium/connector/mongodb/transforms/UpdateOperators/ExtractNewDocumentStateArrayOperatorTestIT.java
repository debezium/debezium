/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.UpdateOperators;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.connector.mongodb.TestHelper;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.data.VerifyRecord;

/**
 * Integration test for {@link ExtractNewDocumentState}.
 * <p>
 * This subset of tests cover the Array Update Operator as in the official documentation
 * {@see https://docs.mongodb.com/v3.6/reference/operator/update-array/#update-operators}
 *
 * @author Renato Mefi
 */
public class ExtractNewDocumentStateArrayOperatorTestIT extends AbstractExtractNewDocumentStateUpdateOperatorsTestIT {

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/addToSet/#up._S_addToSet">MongoDB operator array update $addToSet</a>
     */
    @Test
    public void shouldTransformOperationAddToSet() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$addToSet': {dataArrayOfStr: 'b'}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataArrayOfStr", valueSchema.field("dataArrayOfStr").schema(),
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataArrayOfStr")).isEqualTo(Arrays.asList("a", "c", "e", "b"));
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/pop/#up._S_pop">MongoDB operator array update $pop</a>
     */
    @Test
    public void shouldTransformOperationPop() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$pop': {dataArrayOfStr: -1}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataArrayOfStr", valueSchema.field("dataArrayOfStr").schema(),
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataArrayOfStr")).isEqualTo(Arrays.asList("c", "e"));
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/pull/#pull">MongoDB operator array update $pull</a>
     */
    @Test
    public void shouldTransformOperationPull() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$pull': {dataArrayOfStr: {$in: ['c']}}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataArrayOfStr", valueSchema.field("dataArrayOfStr").schema(),
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataArrayOfStr")).isEqualTo(Arrays.asList("a", "e"));
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/pullAll/#up._S_pullAll">MongoDB operator array update $pullAll</a>
     */
    @Test
    public void shouldTransformOperationPullAll() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$pullAll': {dataArrayOfStr: ['c']}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataArrayOfStr", valueSchema.field("dataArrayOfStr").schema(),
                SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataArrayOfStr")).isEqualTo(Arrays.asList("a", "e"));
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/push/#push">MongoDB operator array update $push</a>
     */
    @Test
    public void shouldTransformOperationPush() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$push': {dataArrayOfStr: 'g'}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        // Operations which include items to arrays result in a new field where the structure looks like "FIELD_NAME.ARRAY_INDEX"
        VerifyRecord.assertConnectSchemasAreEqual("id", valueSchema.field("id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("id")).isEqualTo(1);
        if (TestHelper.isOplogCaptureMode()) {
            VerifyRecord.assertConnectSchemasAreEqual("dataArrayOfStr.3", valueSchema.field("dataArrayOfStr.3").schema(), Schema.OPTIONAL_STRING_SCHEMA);
            assertThat(transformedUpdateValue.get("dataArrayOfStr.3")).isEqualTo("g");
        }
        else {
            VerifyRecord.assertConnectSchemasAreEqual("dataArrayOfStr", valueSchema.field("dataArrayOfStr").schema(),
                    SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
            assertThat(transformedUpdateValue.get("dataArrayOfStr")).isEqualTo(Arrays.asList("a", "c", "e", "g"));
        }
    }
}
