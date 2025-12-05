/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.UpdateOperators;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.data.VerifyRecord;

/**
 * Integration test for {@link ExtractNewDocumentState}.
 * <p>
 * This subset of tests cover the Bitwise Update Operator as in the official documentation
 * {@see https://docs.mongodb.com/v3.6/reference/operator/update/bit/#bit}
 *
 * @author Renato Mefi
 */
public class ExtractNewDocumentStateUpdateBitOperatorTestIT extends AbstractExtractNewDocumentStateUpdateOperatorsTestIT {

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/bit/#bitwise-and">MongoDB operator update $bit AND</a>
     */
    @Test
    public void shouldTransformOperationBitAnd() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$bit': {dataInt: {and: NumberInt(1010)}}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("_id", valueSchema.field("_id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("_id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(114);
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/bit/#bitwise-or">MongoDB operator update $bit OR</a>
     */
    @Test
    public void shouldTransformOperationBitOr() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$bit': {dataInt: {or: NumberInt(1001)}}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("_id", valueSchema.field("_id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("_id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(1019);
    }

    /**
     * @see <a href="https://docs.mongodb.com/v3.6/reference/operator/update/bit/#bitwise-xor">MongoDB operator update $bit XOR</a>
     */
    @Test
    public void shouldTransformOperationBitXor() throws InterruptedException {
        SourceRecord updateRecord = executeSimpleUpdateOperation(
                "{'$bit': {dataInt: {xor: NumberInt(111)}}}");

        final SourceRecord transformedUpdate = transformation.apply(updateRecord);
        final Struct transformedUpdateValue = (Struct) transformedUpdate.value();
        final Schema valueSchema = transformedUpdate.valueSchema();

        VerifyRecord.assertConnectSchemasAreEqual("_id", valueSchema.field("_id").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        VerifyRecord.assertConnectSchemasAreEqual("dataInt", valueSchema.field("dataInt").schema(), Schema.OPTIONAL_INT32_SCHEMA);
        assertThat(transformedUpdateValue.get("_id")).isEqualTo(1);
        assertThat(transformedUpdateValue.get("dataInt")).isEqualTo(20);
    }
}
