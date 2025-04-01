/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.ai.embeddings;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

/**
 * Dummy model factory for testing.
 *
 * @author vjuranek
 */
public class DummyModelFactory implements EmbeddingsModelFactory {

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf();
    }

    @Override
    public void configure(Configuration config) {
        // no-op
    }

    @Override
    public void validateConfiguration() {
        // no-op
    }

    @Override
    public EmbeddingModel getModel() {
        return new DummyEmbeddingModel();
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
