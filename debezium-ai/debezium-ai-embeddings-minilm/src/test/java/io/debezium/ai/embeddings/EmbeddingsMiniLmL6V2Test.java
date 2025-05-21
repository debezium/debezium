/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static io.debezium.ai.embeddings.FieldToEmbedding.LEGACY_EMBEDDINGS_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

/**
 * Basic tests for {@link FieldToEmbedding} SMT which uses {@link MiniLmL6V2ModeFactory}.
 *
 * @author vjuranek
 */
public class EmbeddingsMiniLmL6V2Test {
    @Test
    public void testMiniLmEmbeddings() {
        assertEmbeddingsForConfig(Map.of(
                "field.source", "after.product",
                "field.embedding", "after.prod_embedding"));
    }

    @Test
    public void testMiniLmEmbeddingsWithLegacyConfig() {
        assertEmbeddingsForConfig(Map.of(
                LEGACY_EMBEDDINGS_PREFIX + "field.source", "after.product",
                LEGACY_EMBEDDINGS_PREFIX + "field.embedding", "after.prod_embedding"));
    }

    private void assertEmbeddingsForConfig(Map<String, ?> config) {
        FieldToEmbedding<SourceRecord> embeddingSmt = new FieldToEmbedding();
        embeddingSmt.configure(config);
        SourceRecord transformedRecord = embeddingSmt.apply(FieldToEmbeddingTest.SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        List<Float> embeddings = payloadStruct.getStruct("after").getArray("prod_embedding");
        assertThat(embeddings.size()).isEqualTo(384);
        assertThat(embeddings).startsWith(-0.07157834f, 0.022438003f, -0.0236557f, -0.014294243f, 0.0048211957f, 0.020384153f, 0.20435654f, 0.057306267f, 0.05457874f,
                -0.030588629f);
    }
}
