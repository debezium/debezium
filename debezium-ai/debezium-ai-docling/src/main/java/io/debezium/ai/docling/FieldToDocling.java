/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.docling;

import static java.lang.String.format;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.transforms.ConnectRecordUtil;
import io.debezium.transforms.SmtManager;
import io.debezium.util.BoundedConcurrentHashMap;

import ai.docling.api.serve.DoclingServeApi;
import ai.docling.api.serve.convert.request.ConvertDocumentRequest;
import ai.docling.api.serve.convert.request.options.ConvertDocumentOptions;
import ai.docling.api.serve.convert.request.options.InputFormat;
import ai.docling.api.serve.convert.request.options.OutputFormat;
import ai.docling.api.serve.convert.request.source.FileSource;
import ai.docling.api.serve.convert.request.source.HttpSource;
import ai.docling.api.serve.convert.request.source.Source;
import ai.docling.api.serve.convert.request.target.InBodyTarget;
import ai.docling.api.serve.convert.response.ConvertDocumentResponse;
import ai.docling.client.serve.DoclingServeClientBuilderFactory;

/**
 * Single message transform which appends to the record <a href="https://github.com/docling-project/">Docling</a> transformation of selected {@link String} field
 * or replace this field with it.
 * Docling is able to parse various kinds of inpout formats and convert them to selected output format, making it more easy and efficient to consume these documents
 * by LLMs and AI in general.
 *
 * @author vjuranek
 */
public class FieldToDocling<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldToDocling.class);

    private static final Schema DOCLING_SCHEMA = Schema.STRING_SCHEMA;
    private static final String NESTING_SPLIT_REG_EXP = "\\.";
    private static final int CACHE_SIZE = 64;
    private final BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    private static final Field SOURCE_FIELD = Field.create("field.source")
            .withDisplayName("Name of the record field which should be used as an Docling input.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Name of the record field which should be used as an Docling input. Supports also nested fields.");

    private static final Field DOCLING_FIELD = Field.create("field.docling")
            .withDisplayName("Name of the field which would contain Docling output.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Name of the field which will be appended to the record and which would contain Docling document created from `field.source` field. Supports also nested fields.");

    private static final Field SERVE_URL = Field.create("serve.url")
            .withDisplayName("URL of Docling serve API.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("URL of Docling serve API, a server this provides Docling as a service.");

    private static final Field INPUT_SOURCE = Field.create("input.source")
            .withDisplayName("Docling input source, either 'text' or 'link' value.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Specifies how Docling should treat the source field - it can contain either directly the document to be "
                    + "transformed ('text' option) or a link to the document to be transformed ('link' option).");

    private static final Field INPUT_FORMAT = Field.create("input.format")
            .withDisplayName("Docling input format.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Format of the document to be transformed. Can be any option which Docling supports.");

    private static final Field INCLUDE_IMAGES = Field.create("include.images")
            .withDisplayName("Whether to include images.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault("true")
            .withDescription("Specifies if the images should or shouldn't be included into the resulting document.");

    private static final Field OUTPUT_FORMAT = Field.create("output.format")
            .withDisplayName("Docling output format.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withAllowedValues(new HashSet<>(Arrays.asList(SupportedOutputFormat.values())))
            .withDescription("Format of the document provided by the Docling. Can be on of 'html', 'markdown' or 'text'.");

    public static final Field.Set ALL_FIELDS = Field.setOf(SOURCE_FIELD, DOCLING_FIELD, SERVE_URL, INPUT_SOURCE, INPUT_FORMAT, INCLUDE_IMAGES, OUTPUT_FORMAT);

    private SmtManager<R> smtManager;
    private String sourceField;
    private String doclingField;
    private String serverUrl;
    private InputSource inputSource;
    private InputFormat inputFormat;
    private boolean includeImages;
    private SupportedOutputFormat outputFormat;
    private List<String> sourceFieldPath;
    DoclingServeApi doclingServeApi;

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
        Field.group(config, null, SOURCE_FIELD, DOCLING_FIELD, SERVE_URL, INPUT_SOURCE, INPUT_FORMAT, INCLUDE_IMAGES, OUTPUT_FORMAT);
        return config;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, ALL_FIELDS);

        sourceField = config.getString(SOURCE_FIELD);
        doclingField = config.getString(DOCLING_FIELD);
        serverUrl = config.getString(SERVE_URL);
        inputSource = InputSource.parse(config.getString(INPUT_SOURCE));
        inputFormat = parseInputFormat(config.getString(INPUT_FORMAT));
        includeImages = config.getBoolean(INCLUDE_IMAGES);
        outputFormat = SupportedOutputFormat.parseOutputFormat(config.getString(OUTPUT_FORMAT));
        validateConfiguration();

        sourceFieldPath = Arrays.asList(sourceField.split(NESTING_SPLIT_REG_EXP));
        doclingServeApi = DoclingServeClientBuilderFactory.newBuilder().baseUrl(serverUrl).build();
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
            throw new ConfigException(format("'%s' must be set to non-empty value.", SOURCE_FIELD));
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
            for (int i = 0; i < sourceFieldPath.size() - 1; i++) {
                if (struct.schema().type() == Schema.Type.STRUCT) {
                    struct = struct.getStruct(sourceFieldPath.get(i));
                    if (struct == null) {
                        LOGGER.debug("Skipping record {}, the structure is not present", record);
                        return null;
                    }
                }
                else {
                    throw new IllegalArgumentException(format("Invalid field name %s, %s is not struct.", sourceField, struct.schema().name()));
                }
            }
            return struct.getString(sourceFieldPath.getLast());
        }
        else {
            LOGGER.debug("Skipping record {}, it has either null value or invalid structure", record);
        }
        return null;
    }

    /**
     * Copies the original record and appends Docling transformation of the text to the record.
     */
    protected R buildUpdatedRecord(R original, String fieldInput) {
        final Struct value = requireStruct(original.value(), "Original value must be struct");

        Source source = switch (inputSource) {
            case TEXT -> {
                String fieldInputEncoded = Base64.getEncoder().encodeToString(fieldInput.getBytes(StandardCharsets.UTF_8));
                String filename = String.format("debezium-smt-%s.%s", UUID.randomUUID(), inputFormat.name());
                yield FileSource.builder().base64String(fieldInputEncoded).filename(filename).build();
            }
            case LINK -> HttpSource.builder().url(URI.create(fieldInput)).build();
        };

        ConvertDocumentRequest request = ConvertDocumentRequest.builder()
                .source(source)
                .options(ConvertDocumentOptions.builder()
                        .toFormat(outputFormat.getOutputFormat())
                        .includeImages(includeImages)
                        .build())
                .target(InBodyTarget.builder().build())
                .build();

        ConvertDocumentResponse response = doclingServeApi.convertSource(request);
        String doclingContent = switch (outputFormat) {
            case HTML -> response.getDocument().getHtmlContent();
            case MARKDOWN -> response.getDocument().getMarkdownContent();
            case TEXT -> response.getDocument().getTextContent();
        };

        final Schema updatedSchema;
        final Object updatedValue;
        if (doclingField == null) {
            updatedSchema = DOCLING_SCHEMA;
            updatedValue = doclingContent;
        }
        else {
            final List<ConnectRecordUtil.NewEntry> newEntries = List.of(new ConnectRecordUtil.NewEntry(doclingField, DOCLING_SCHEMA, doclingContent));
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

    private enum InputSource implements EnumeratedValue {

        TEXT("text"),
        LINK("link");

        private final String source;

        InputSource(String source) {
            this.source = source;
        }

        @Override
        public String getValue() {
            return source;
        }

        public static InputSource parse(String source) {
            if (source == null || source.isEmpty()) {
                return TEXT;
            }

            for (InputSource s : InputSource.values()) {
                if (s.source.equalsIgnoreCase(source)) {
                    return s;
                }
            }

            return TEXT;
        }
    }

    private enum SupportedOutputFormat implements EnumeratedValue {

        HTML("html", OutputFormat.HTML),
        MARKDOWN("markdown", OutputFormat.MARKDOWN),
        TEXT("text", OutputFormat.TEXT);

        private final String name;
        private final OutputFormat outputFormat;

        SupportedOutputFormat(String name, OutputFormat outputFormat) {
            this.name = name;
            this.outputFormat = outputFormat;
        }

        public static SupportedOutputFormat parseOutputFormat(String outputFormat) {
            if (outputFormat == null || outputFormat.isEmpty()) {
                return TEXT;
            }

            for (SupportedOutputFormat f : SupportedOutputFormat.values()) {
                if (outputFormat.equalsIgnoreCase(f.name())) {
                    return f;
                }
            }

            return TEXT;
        }

        @Override
        public String getValue() {
            return name;
        }

        public OutputFormat getOutputFormat() {
            return outputFormat;
        }
    }

    private InputFormat parseInputFormat(String inputFormat) {
        if (inputFormat == null || inputFormat.isEmpty()) {
            return InputFormat.ASCIIDOC;
        }

        for (InputFormat f : InputFormat.values()) {
            if (inputFormat.equalsIgnoreCase(f.name())) {
                return f;
            }
        }

        return InputFormat.ASCIIDOC;
    }
}
