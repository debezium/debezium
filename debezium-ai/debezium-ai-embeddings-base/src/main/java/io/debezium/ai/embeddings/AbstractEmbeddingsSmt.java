/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.embeddings;

import static java.lang.String.format;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.vector.FloatVector;
import io.debezium.transforms.ConnectRecordUtil;
import io.debezium.transforms.SmtManager;
import io.debezium.util.BoundedConcurrentHashMap;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;

/**
 * Base class of the embeddings SMT, which provides common configuration and manipulations with the Connect record.
 * Child classes has to implement at least {@link AbstractEmbeddingsSmt#getModel()} class, which provides specific
 * model which will be used for creating the embeddings.
 *
 * @author vjuranek
 */
public abstract class AbstractEmbeddingsSmt<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEmbeddingsSmt.class);

    private static final Field TEXT_FIELD = Field.create("field.source")
            .withDisplayName("Name of the record field from which embeddings should be created.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Name of the field of the record which content will be used as an input for embeddings. Supports also nested fields.");

    private static final Field EMBEDDGINS_FIELD = Field.create("field.embeddings")
            .withDisplayName("Name of the field which would contain the embeddings of the input field.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription(
                    "Name of the field which which will be appended to the record and which would contain the embeddings of the content `filed.source` field. Supports also nested fields.");

    private static final Field.Set ALL_FIELDS = Field.setOf(TEXT_FIELD, EMBEDDGINS_FIELD);
    private static final Schema VECTOR_SCHEMA = FloatVector.schema();

    private SmtManager<R> smtManager;
    private String sourceField;
    private String embeddingsField;
    private List<String> sourceFiledPath;
    private EmbeddingModel model;

    private static final String NESTING_SPLIT_REG_EXP = "\\.";
    private static final int CACHE_SIZE = 64;
    private final BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    public abstract EmbeddingModel getModel();

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(TEXT_FIELD, EMBEDDGINS_FIELD));
        sourceField = config.getString(TEXT_FIELD);
        embeddingsField = config.getString(EMBEDDGINS_FIELD);

        validateConfiguration();
        sourceFiledPath = Arrays.asList(sourceField.split(NESTING_SPLIT_REG_EXP));
        model = getModel();
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            LOGGER.trace("Record {} has null value of invalid envelope and will be skipped.", record.value());
            return record;
        }

        final String text = getSourceString(record);
        return text == null ? record : buildUpdatedRecord(record, text);
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, TEXT_FIELD, EMBEDDGINS_FIELD);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    private void validateConfiguration() {
        if (sourceField.isBlank() || embeddingsField.isBlank()) {
            throw new ConfigException(format("Both '%s' and '%s' must be set to non-empty value.", TEXT_FIELD, EMBEDDGINS_FIELD));
        }
    }

    /**
     *
     * Based on the configuration, obtains value of the record field from which the embeddings will be computed.
     * This field has to be of type {@link String}.
     */
    protected String getSourceString(R record) {
        if (record.value() != null && smtManager.isValidEnvelope(record) && record.valueSchema().type() == Schema.Type.STRUCT) {
            Struct struct = requireStruct(record.value(), "Obtaining source field for embeddings");
            for (int i = 0; i < sourceFiledPath.size() - 1; i++) {
                if (struct.schema().type() == Schema.Type.STRUCT) {
                    struct = struct.getStruct(sourceFiledPath.get(i));
                }
                else {
                    throw new IllegalArgumentException(format("Invalid field name %s, %s is not struct.", sourceField, struct.schema().name()));
                }
            }
            return struct.getString(sourceFiledPath.getLast());
        }
        else {
            LOGGER.debug("Skipping record {}, it has either null value or invalid structure", record);
        }
        return null;
    }

    /**
     * Copies the original record and appends to it embeddings of the text contained in the source field of the records.
     */
    protected R buildUpdatedRecord(R original, String text) {
        final Struct value = (Struct) original.value();
        final TextSegment segment = TextSegment.from(text);
        final Embedding embedding = model.embed(segment).content();

        final List<ConnectRecordUtil.NewEntry> newEntries = List.of(new ConnectRecordUtil.NewEntry(embeddingsField, VECTOR_SCHEMA, embedding.vectorAsList()));
        final Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(), valueSchema -> ConnectRecordUtil.makeNewSchema(valueSchema, newEntries));
        final Struct updatedValue = ConnectRecordUtil.makeUpdatedValue(value, newEntries, updatedSchema);

        return original.newRecord(
                original.topic(),
                original.kafkaPartition(),
                original.keySchema(),
                original.key(),
                updatedSchema,
                updatedValue,
                original.timestamp(),
                original.headers());
    }
}
