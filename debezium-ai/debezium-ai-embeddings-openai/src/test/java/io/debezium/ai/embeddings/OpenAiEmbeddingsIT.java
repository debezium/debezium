/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import dev.langchain4j.model.openai.OpenAiEmbeddingModelName;

/**
 * Integration tests for {@link FieldToEmbedding} SMT which uses {@link OpenAiModelFactory}.
 *
 * @author kmos
 */
@EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+")
public class OpenAiEmbeddingsIT {

    @Test
    public void testOpenAiEmbeddings() {
        FieldToEmbedding<SourceRecord> embeddingSmt = new FieldToEmbedding();
        embeddingSmt.configure(Map.of(
                "field.source", "after.product",
                "field.embedding", "after.prod_embedding",
                "openai.api.key", System.getenv("OPENAI_API_KEY"),
                "openai.model.name", OpenAiEmbeddingModelName.TEXT_EMBEDDING_3_SMALL.toString()));
        SourceRecord transformedRecord = embeddingSmt.apply(FieldToEmbeddingTest.SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        List<Float> embeddings = payloadStruct.getStruct("after").getArray("prod_embedding");
        assertThat(embeddings.size()).isEqualTo(1536);
    }

    @Test
    public void testOpenAiEmbeddingsWithCustomBaseUrl() {
        FieldToEmbedding<SourceRecord> embeddingSmt = new FieldToEmbedding();
        embeddingSmt.configure(Map.of(
                "field.source", "after.product",
                "field.embedding", "after.prod_embedding",
                "openai.api.key", System.getenv("OPENAI_API_KEY"),
                "openai.model.name", OpenAiEmbeddingModelName.TEXT_EMBEDDING_3_SMALL.toString(),
                "openai.baseUrl", "https://api.openai.com/v1"));
        SourceRecord transformedRecord = embeddingSmt.apply(FieldToEmbeddingTest.SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        List<Float> embeddings = payloadStruct.getStruct("after").getArray("prod_embedding");
        assertThat(embeddings.size()).isEqualTo(1536);
    }
}