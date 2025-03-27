/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

/**
 * @author vjuranek
 */
public class AbstractEmbeddingsTransformationTest {
    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("mysql.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .build();

    public static final Struct ROW = new Struct(VALUE_SCHEMA)
            .put("id", 101L)
            .put("price", 20.0F)
            .put("product", "a product");

    public static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("mysql.inventory.products.Envelope")
            .withRecord(VALUE_SCHEMA)
            .withSource(Schema.STRING_SCHEMA)
            .build();

    public static final Struct PAYLOAD = ENVELOPE.create(ROW, null, Instant.now());
    public static final SourceRecord SOURCE_RECORD = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", ENVELOPE.schema(), PAYLOAD);

    private final DummyEmbeddingSmt embeddingSmt = new DummyEmbeddingSmt();

    @Test
    public void testNonNestedFieldIsEmbeddedNonNested() {
        embeddingSmt.configure(Map.of(
                "field.source", "op",
                "field.embeddings", "op_embedding"));
        SourceRecord transformedRecord = embeddingSmt.apply(SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getString("op")).isEqualTo("c");
        assertThat(payloadStruct.getArray("op_embedding")).contains(0.0f, 1.0f, 2.0f, 3.0f);
    }

    @Test
    public void testNestedFieldIsEmbeddedNested() {
        embeddingSmt.configure(Map.of(
                "field.source", "after.product",
                "field.embeddings", "after.prod_embedding"));
        SourceRecord transformedRecord = embeddingSmt.apply(SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        assertThat(payloadStruct.getStruct("after").getArray("prod_embedding")).contains(0.0f, 1.0f, 2.0f, 3.0f);
    }

    /**
     * Implementation of {@link AbstractEmbeddingsTransformation}, which provides dummy embeddings model for basic testing.
     */
    private static class DummyEmbeddingSmt extends AbstractEmbeddingsTransformation<SourceRecord> {
        @Override
        public EmbeddingModel getModel() {
            return new DummyEmbeddingModel();
        }

        @Override
        public String version() {
            return "0";
        }
    }

    /**
     * Implementation of {@link EmbeddingModel} which returns constant vector for any input.
     */
    private static class DummyEmbeddingModel implements EmbeddingModel {
        @Override
        public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
            float[] dummyVector = new float[]{ 0.f, 1.f, 2.f, 3.f };
            return new Response<>(List.of(new Embedding(dummyVector)));
        }
    }
}
