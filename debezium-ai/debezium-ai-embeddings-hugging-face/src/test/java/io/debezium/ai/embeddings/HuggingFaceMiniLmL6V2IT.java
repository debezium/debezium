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

/**
 * Basic tests for {@link FieldToEmbedding} SMT which uses {@link HuggingFaceModelFactory}.
 *
 * @author vjuranek
 */
@EnabledIfEnvironmentVariable(named = "HF_API_KEY", matches = ".+")
public class HuggingFaceMiniLmL6V2IT {

    @Test
    public void testMiniLmEmbeddings() {
        FieldToEmbedding<SourceRecord> embeddingSmt = new FieldToEmbedding();
        embeddingSmt.configure(Map.of(
                "field.source", "after.product",
                "field.embedding", "after.prod_embedding",
                "huggingface.access.token", System.getenv("HF_API_KEY"),
                "huggingface.model.name", "sentence-transformers/all-MiniLM-L6-v2"));
        SourceRecord transformedRecord = embeddingSmt.apply(FieldToEmbeddingTest.SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        List<Float> embeddings = payloadStruct.getStruct("after").getArray("prod_embedding");
        assertThat(embeddings.size()).isEqualTo(384);
        assertThat(embeddings).startsWith(-0.07157832f, 0.02243797f, -0.02365577f, -0.014294202f, 0.0048212307f, 0.020384131f, 0.20435654f, 0.057306245f, 0.054578826f,
                -0.030588612f);
    }
}
