/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;

/**
 * Embedding model factory which uses <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2">MiniLmL6V2 model</a> for creating embeddings.
 *
 * @author vjuranek
 */
public class MiniLmL6V2ModeFactory<R extends ConnectRecord<R>> implements EmbeddingsModelFactory {

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
        return new AllMiniLmL6V2EmbeddingModel();
    }
}
