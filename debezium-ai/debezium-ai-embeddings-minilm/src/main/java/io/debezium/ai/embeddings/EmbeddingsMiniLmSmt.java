/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import org.apache.kafka.connect.connector.ConnectRecord;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;

/**
 * Embeddings SMT which uses <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2">MiniLmL6V2 model</a> model for creating embeddings.
 *
 * @author vjuranek
 */
public class EmbeddingsMiniLmSmt<R extends ConnectRecord<R>> extends AbstractEmbeddingsSmt<R> {
    public EmbeddingModel getModel() {
        return new AllMiniLmL6V2EmbeddingModel();
    }
}
