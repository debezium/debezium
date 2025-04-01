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
import org.junit.Test;

/**
 * @author vjuranek
 */
public class EmbeddingsMiniLmL6V2Test {
    private final EmbeddingsMiniLmL6V2<SourceRecord> embeddingSmt = new EmbeddingsMiniLmL6V2<>();

    @Test
    public void testMiniLmEmbeddings() {
        embeddingSmt.configure(Map.of(
                "embeddings.field.source", "after.product",
                "embeddings.field.embedding", "after.prod_embedding"));
        SourceRecord transformedRecord = embeddingSmt.apply(AbstractEmbeddingsTransformationTest.SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("product")).contains("a product");
        List<Float> embeddings = payloadStruct.getStruct("after").getArray("prod_embedding");
        assertThat(embeddings.size()).isEqualTo(384);
        assertThat(embeddings).startsWith(-0.07157834f, 0.022438003f, -0.0236557f, -0.014294243f, 0.0048211957f, 0.020384153f, 0.20435654f, 0.057306267f, 0.05457874f,
                -0.030588629f);
    }
}
