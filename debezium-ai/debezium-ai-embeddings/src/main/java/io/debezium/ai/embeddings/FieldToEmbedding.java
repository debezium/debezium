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
import java.util.Optional;
import java.util.ServiceLoader;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
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
 * Single message transform which appends to the record payload embedding of selected {@link String} field.
 * Embedding model is provided via appropriate {@link EmbeddingsModelFactory} factory, which under the hood
 * uses <a href="https://docs.langchain4j.dev/">LangChain4j</a> project. Model factory class is loaded via
 * SPI. You have to place one of the Debezium AI Embeddings modules with model of your choice on the class
 * path or implement your own model factory provider and place it on the class path.
 *
 * @author vjuranek
 */
public class FieldToEmbedding<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldToEmbedding.class);

    public static final String EMBEDDINGS_PREFIX = "embeddings.";

    private static final Field TEXT_FIELD = Field.create(EMBEDDINGS_PREFIX + "field.source")
            .withDisplayName("Name of the record field from which embeddings should be created.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Name of the field of the record which content will be used as an input for embeddings. Supports also nested fields.");

    private static final Field EMBEDDGINS_FIELD = Field.create(EMBEDDINGS_PREFIX + "field.embedding")
            .withDisplayName("Name of the field which would contain the embeddings of the input field.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Name of the field which which will be appended to the record and which would contain the embeddings of the content `filed.source` field. Supports also nested fields.");

    private static final Schema EMBEDDING_SCHEMA = FloatVector.schema();
    private static final EmbeddingsModelFactory MODEL_FACTORY = EmbeddingsModelFactoryLoader.getModelFactory();
    public static final Field.Set ALL_FIELDS = Field.setOf(TEXT_FIELD, EMBEDDGINS_FIELD).with(MODEL_FACTORY.getConfigFields());

    private SmtManager<R> smtManager;
    private String sourceField;
    private String embeddingsField;
    private List<String> sourceFiledPath;
    private EmbeddingModel model;

    private static final String NESTING_SPLIT_REG_EXP = "\\.";
    private static final int CACHE_SIZE = 64;
    private final BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, ALL_FIELDS);

        sourceField = config.getString(TEXT_FIELD);
        embeddingsField = config.getString(EMBEDDGINS_FIELD);
        MODEL_FACTORY.configure(config);
        validateConfiguration();

        sourceFiledPath = Arrays.asList(sourceField.split(NESTING_SPLIT_REG_EXP));
        model = MODEL_FACTORY.getModel();
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

    protected void validateConfiguration() {
        if (sourceField == null || sourceField.isBlank()) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", TEXT_FIELD));
        }
        MODEL_FACTORY.validateConfiguration();
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
        final Struct value = requireStruct(original.value(), "Original value must be struct");
        final TextSegment segment = TextSegment.from(text);
        final Embedding embedding = model.embed(segment).content();

        final Schema updatedSchema;
        final Object updatedValue;
        if (embeddingsField == null) {
            updatedSchema = EMBEDDING_SCHEMA;
            updatedValue = embedding.vectorAsList();
        }
        else {
            final List<ConnectRecordUtil.NewEntry> newEntries = List.of(new ConnectRecordUtil.NewEntry(embeddingsField, EMBEDDING_SCHEMA, embedding.vectorAsList()));
            updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(), valueSchema -> ConnectRecordUtil.makeNewSchema(valueSchema, newEntries));
            updatedValue = ConnectRecordUtil.makeUpdatedValue(value, newEntries, updatedSchema);
        }

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

    /**
     * {@link EmbeddingsModelFactory} loader which loads model factory supplied by the user via SPI.
     */
    public static class EmbeddingsModelFactoryLoader<R extends ConnectRecord<R>> {
        private static final Logger LOGGER = LoggerFactory.getLogger(FieldToEmbedding.EmbeddingsModelFactoryLoader.class);

        static EmbeddingsModelFactory getModelFactory() {
            final ServiceLoader<EmbeddingsModelFactory> loader = ServiceLoader.load(EmbeddingsModelFactory.class);
            final Optional<EmbeddingsModelFactory> factory = loader.findFirst();
            if (factory.isEmpty()) {
                throw new DebeziumException("No implementation of Debezium embeddings model factory found.");
            }
            if (loader.stream().count() > 1) {
                LOGGER.warn(
                        "More then one Debezium embeddings model factory found. Order of loading is not defined and you may load different factory than you intended.");
                LOGGER.warn("Found following factories:");
                loader.stream().forEach(f -> LOGGER.warn(f.get().getClass().getName()));
            }
            return factory.get();
        }
    }
}
