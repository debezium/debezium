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

import dev.langchain4j.model.voyageai.VoyageAiEmbeddingModelName;

/**
 * Basic tests for {@link FieldToEmbedding} SMT which uses {@link VoyageAiModelFactory}.
 *
 * @author vjuranek
 */
@EnabledIfEnvironmentVariable(named = "VOYAGE_API_KEY", matches = ".+")
public class Voyage3LiteIT {
    @Test
    public void testMiniLmEmbeddings() {
        FieldToEmbedding<SourceRecord> embeddingSmt = new FieldToEmbedding();
        embeddingSmt.configure(Map.of(
                "field.source", "after.product",
                "field.embedding", "after.prod_embedding",
                "voyageai.access.token", System.getenv("VOYAGE_API_KEY"),
                "voyageai.model.name", VoyageAiEmbeddingModelName.VOYAGE_3_LITE));
        SourceRecord transformedRecord = embeddingSmt.apply(FieldToEmbeddingTest.SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        List<Float> embeddings = payloadStruct.getStruct("after").getArray("prod_embedding");
        assertThat(embeddings.size()).isEqualTo(512);
        assertThat(embeddings).startsWith(0.05524283f, 0.051374566f, -0.01567531f, -2.40186E-4f, 0.030642705f, 0.004652028f, -0.08393876f, 0.07099399f, 0.052891534f,
                0.003741849f);
    }
}
