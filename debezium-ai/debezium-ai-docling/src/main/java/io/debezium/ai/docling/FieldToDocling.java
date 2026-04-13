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

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.transforms.ConnectRecordUtil;
import io.debezium.transforms.SmtManager;
import io.debezium.util.BoundedConcurrentHashMap;

import ai.docling.serve.api.DoclingServeApi;
import ai.docling.serve.api.convert.request.ConvertDocumentRequest;
import ai.docling.serve.api.convert.request.options.ConvertDocumentOptions;
import ai.docling.serve.api.convert.request.options.InputFormat;
import ai.docling.serve.api.convert.request.options.OutputFormat;
import ai.docling.serve.api.convert.request.source.FileSource;
import ai.docling.serve.api.convert.request.source.HttpSource;
import ai.docling.serve.api.convert.request.source.Source;
import ai.docling.serve.api.convert.request.target.InBodyTarget;
import ai.docling.serve.api.convert.response.InBodyConvertDocumentResponse;
import ai.docling.serve.client.DoclingServeClientBuilderFactory;

/**
 * Single message transform which appends to the record <a href="https://github.com/docling-project/">Docling</a> transformation of selected {@link String} field
 * or replace entire record with Docling record.
 * Docling is able to parse various kinds of input formats and convert them to selected output format, making it more easy and efficient to consume these documents
 * by LLMs and AI in general.
 *
 * @author vjuranek
 */
public class FieldToDocling<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldToDocling.class);

    private static final String NESTING_SPLIT_REG_EXP = "\\.";
    private static final int CACHE_SIZE = 64;
    private final Map<Object, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    private static final Field SOURCE_FIELD = Field.create("field.source")
            .withDisplayName("Name of the record field which should be used as a Docling input.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Name of the record field which should be used as a Docling input. Supports also nested fields.");

    private static final Field DOCLING_FIELD = Field.create("field.docling")
            .withDisplayName("Name of the field which would contain Docling output.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Name of the field which will be appended to the record and which would contain Docling document created from `field.source` field. Supports also nested fields, but the nested structure has to exists. If the field is not specified, records will be replaced by record containing only Docling document.");

    private static final Field SERVE_URL = Field.create("serve.url")
            .withDisplayName("URL of Docling Serve API.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("URL of Docling Serve API, a server this provides Docling as a service.");

    private static final Field INPUT_SOURCE = Field.create("input.source")
            .withDisplayName("Docling input source, either 'text' or 'link' value.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withEnum(InputSource.class, InputSource.TEXT)
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
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(true)
            .withDescription("Specifies if the images should or shouldn't be included into the resulting document.");

    private static final Field OUTPUT_FORMAT = Field.create("output.format")
            .withDisplayName("Docling output format.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withEnum(SupportedOutputFormat.class, SupportedOutputFormat.TEXT)
            .withDescription("Format of the document provided by the Docling. Can be one of 'html', 'markdown' or 'text'.");

    private static final Field SIMPLE_SCHEMA_LOOKUP = Field.create("simple.schema.lookup")
            .withDisplayName("Use simple schema lookup.")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(false)
            .withDescription(
                    "Adding Docling field requires schema update. Debezium caches the schemas, but even cache lookup can be expensive. "
                            + "If the schema doesn't change over time, the schema lookup can be looked up in the cache by its name. Turn on only when "
                            + "you are sure the schema evolution is not happening during the Debezium run.");

    public static final Field.Set ALL_FIELDS = Field.setOf(SOURCE_FIELD, DOCLING_FIELD, SERVE_URL, INPUT_SOURCE, INPUT_FORMAT, INCLUDE_IMAGES, OUTPUT_FORMAT,
            SIMPLE_SCHEMA_LOOKUP);

    private SmtManager<R> smtManager;
    private String sourceField;
    private String doclingField;
    private String serveUrl;
    private InputSource inputSource;
    private InputFormat inputFormat;
    private boolean includeImages;
    private SupportedOutputFormat outputFormat;
    private List<String> sourceFieldPath;
    private boolean simpleSchemaLookup;
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
        Field.group(config, null, SOURCE_FIELD, DOCLING_FIELD, SERVE_URL, INPUT_SOURCE, INPUT_FORMAT, INCLUDE_IMAGES, OUTPUT_FORMAT, SIMPLE_SCHEMA_LOOKUP);
        return config;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, ALL_FIELDS);

        sourceField = config.getString(SOURCE_FIELD);
        doclingField = config.getString(DOCLING_FIELD);
        serveUrl = config.getString(SERVE_URL);
        inputSource = InputSource.parse(config.getString(INPUT_SOURCE));
        inputFormat = parseInputFormat(config.getString(INPUT_FORMAT));
        includeImages = config.getBoolean(INCLUDE_IMAGES);
        outputFormat = SupportedOutputFormat.parseOutputFormat(config.getString(OUTPUT_FORMAT));
        simpleSchemaLookup = config.getBoolean(SIMPLE_SCHEMA_LOOKUP);
        validateConfiguration();

        sourceFieldPath = Arrays.asList(sourceField.split(NESTING_SPLIT_REG_EXP));
        doclingServeApi = DoclingServeClientBuilderFactory.newBuilder().baseUrl(serveUrl).build();
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return ALL_FIELDS;
    }

    protected void validateConfiguration() {
        if (sourceField == null || sourceField.isBlank()) {
            throw new ConfigException(format("'%s' must be set to non-empty value.", SOURCE_FIELD));
        }
        validateUrl(serveUrl);
    }

    /**
     * Validate URL provided by the user.
     * Allows only HTTP(S) links to prevent e.g. files on the disk.
     */
    protected void validateUrl(String urlString) {
        if (urlString == null || urlString.isBlank()) {
            throw new DebeziumException("URL is empty");
        }

        try {
            URI uri = URI.create(urlString);
            String scheme = uri.getScheme();
            if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
                throw new DebeziumException(format(
                        "URI scheme '%s' is not allowed. Only 'http' and 'https' schemes are permitted for security reasons.",
                        scheme));
            }
        }
        catch (IllegalArgumentException e) {
            throw new DebeziumException(format("Invalid URL: %s", urlString), e);
        }
    }

    /**
     *
     * Based on the configuration, obtains value of the record field from which the Docling document will be computed.
     * This field has to be of type {@link String}.
     */
    protected String getSourceString(R record) {
        if (record.value() != null && smtManager.isValidEnvelope(record) && record.valueSchema().type() == Schema.Type.STRUCT) {
            Struct struct = requireStruct(record.value(), "Obtaining source field for Docling SMT");
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
                String filename = format("debezium-smt-%s.%s", UUID.randomUUID(), inputFormat.name());
                yield FileSource.builder().base64String(fieldInputEncoded).filename(filename).build();
            }
            case LINK -> {
                validateUrl(fieldInput);
                yield HttpSource.builder().url(URI.create(fieldInput)).build();
            }
        };

        ConvertDocumentRequest request = ConvertDocumentRequest.builder()
                .source(source)
                .options(ConvertDocumentOptions.builder()
                        .toFormat(outputFormat.getOutputFormat())
                        .includeImages(includeImages)
                        .build())
                .target(InBodyTarget.builder().build())
                .build();

        InBodyConvertDocumentResponse response = (InBodyConvertDocumentResponse) doclingServeApi.convertSource(request);
        String doclingContent = switch (outputFormat) {
            case HTML -> response.getDocument().getHtmlContent();
            case MARKDOWN -> response.getDocument().getMarkdownContent();
            case TEXT -> response.getDocument().getTextContent();
        };

        final Struct doclingDocument = DoclingDocument.doclingContent(outputFormat.getValue(), doclingContent);
        final Schema updatedSchema;
        final Object updatedValue;
        if (doclingField == null) {
            updatedSchema = doclingDocument.schema();
            updatedValue = doclingDocument;
        }
        else {
            final List<ConnectRecordUtil.NewEntry> newEntries = List.of(new ConnectRecordUtil.NewEntry(doclingField, doclingDocument.schema(), doclingDocument));
            final Schema oldSchema = value.schema();
            final Object cacheKey = simpleSchemaLookup ? value.schema().name() : value.schema();
            updatedSchema = schemaUpdateCache.computeIfAbsent(cacheKey, valueSchema -> ConnectRecordUtil.makeNewSchema(oldSchema, newEntries));
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

            throw new DebeziumException(format("Invalid input source %s", source));
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

            throw new DebeziumException(format("Invalid output format %s", outputFormat));
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

        throw new DebeziumException(format("Invalid input format %s", inputFormat));
    }
}
