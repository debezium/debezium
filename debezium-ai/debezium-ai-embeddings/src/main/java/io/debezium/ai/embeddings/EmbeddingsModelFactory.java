/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.ai.embeddings;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

import dev.langchain4j.model.embedding.EmbeddingModel;

/**
 * Implementations of this interface provide various kind of embedding models as provided by various modules of LangChain4j.
 * Appropriate factory is loaded via SPI in {@link FieldToEmbedding} SMT.
 * Model configuration is part of SMT configuration.
 *
 * @author vjuranek
 */
public interface EmbeddingsModelFactory {

    Field.Set getConfigFields();

    void configure(Configuration config);

    void validateConfiguration();

    EmbeddingModel getModel();
}
