/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.data.SchemaUtil;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.util.Strings;

/**
 * A Kafka Connect SMT that serializes selected record-value fields to JSON strings.
 * <p>
 * Some sinks accept a JSON <em>string</em> but reject a native struct, array or map for a
 * schema-flexible column, for example a Databricks Delta {@code VARIANT} column or a JSON/JSONB
 * column. After a document is flattened, such as by the MongoDB {@code ExtractNewDocumentState}
 * transformation, nested fields arrive as native structs and arrays, which those columns cannot
 * accept directly.
 * <p>
 * This transformation converts the configured fields into their JSON-string representation, so that
 * scalar fields remain strongly typed while a schema-flexible field is written as JSON text. Fields
 * that are already strings, or that are absent, are left untouched. Decimal values nested in a
 * serialized field are written as JSON numbers.
 * <p>
 * Configuration: {@code fields} &mdash; a comma-separated list of value field names to serialize. A
 * name may address a nested field with dot notation, for example {@code after.payload}, which lets
 * the transformation reach into a change-event envelope without a prior flattening step. Each path
 * segment except the last must resolve to a {@code STRUCT}; the final segment is the field whose
 * value is retyped to a JSON string. A bare name such as {@code payload} targets a top-level field,
 * so existing configurations keep working unchanged.
 * <p>
 * When a nested target lives inside a structure that is shared under more than one field of the same
 * named schema &mdash; most notably a change-event envelope's {@code before} and {@code after}, which
 * share one record schema &mdash; the same path must be configured under every such field (for
 * example {@code after.payload} <em>and</em> {@code before.payload}). Otherwise the retyped and the
 * original copies of that schema would carry the same name but different field types, which the
 * Connect schema model does not allow, so the transformation fails with a {@link ConnectException}
 * that names the schema and the fields where it occurs.
 * <p>
 * If a targeted field cannot be serialized to JSON, the transformation throws a {@link ConnectException}
 * that identifies the field, its schema type and name, and the value class, without the value itself.
 * The stream then halts and the offset is not committed past the record, so that no data is dropped
 * silently. To skip such records instead, set {@code errors.tolerance=all}.
 *
 * @param <R> the type of {@link ConnectRecord} that the transformation applies to
 */
public class StringifyFields<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyFields.class);

    public static final String FIELDS_CONFIG = "fields";

    private static final Field FIELDS_FIELD = Field.create(FIELDS_CONFIG)
            .withDisplayName("Fields to serialize")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withValidation(StringifyFields::validateFieldPaths)
            .withDescription("Comma-separated list of value field names to serialize to a JSON string. "
                    + "Use dot notation, such as 'after.payload', to target a nested field.");

    /**
     * A node in the trie of configured field paths. {@link #target} marks a node whose field is
     * itself serialized; {@link #children} holds the next path segment for deeper targets.
     */
    private static final class PathNode {
        private boolean target;
        private final Map<String, PathNode> children = new LinkedHashMap<>();
    }

    private PathNode root;
    private final JsonConverter jsonConverter = new JsonConverter();
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELDS_FIELD));

        this.root = new PathNode();
        for (String path : config.getList(FIELDS_FIELD)) {
            if (Strings.isNullOrBlank(path)) {
                continue;
            }
            PathNode node = root;
            for (String segment : path.split("\\.", -1)) {
                node = node.children.computeIfAbsent(segment.trim(), k -> new PathNode());
            }
            node.target = true;
        }
        // schemas.enable=false emits plain JSON without the Connect schema envelope.
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", false);
        converterConfig.put("converter.type", "value");
        // Write decimals as JSON numbers rather than the default base64-encoded bytes.
        converterConfig.put("decimal.format", "NUMERIC");
        jsonConverter.configure(converterConfig);

        // The retyped schema depends only on the input schema and the configured paths, both fixed
        // after configuration, so it is cached per input schema to avoid rebuilding it per record and
        // to keep a stable schema identity for downstream converters and the schema registry.
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct)) {
            return record;
        }
        Struct value = (Struct) record.value();
        Schema newSchema = schemaUpdateCache.get(value.schema());
        if (newSchema == null) {
            final Set<String> retypedSchemaNames = new HashSet<>();
            newSchema = retypeSchema(value.schema(), root, "", retypedSchemaNames);
            if (!retypedSchemaNames.isEmpty()) {
                checkRetypedSchemaNames(newSchema, retypedSchemaNames, new HashMap<>(), new HashMap<>(), "");
            }
            schemaUpdateCache.put(value.schema(), newSchema);
        }
        if (newSchema == value.schema()) {
            // No configured path applies to this schema, for example a heartbeat or another topic
            return record;
        }
        Struct newValue = transformStruct(value, newSchema, root, "");

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                newSchema, newValue, record.timestamp(), record.headers());
    }

    /**
     * Builds a copy of {@code schema} in which every targeted field is retyped to STRING, descending
     * into nested structs for paths that reach deeper than the current level. Returns {@code schema}
     * itself when no field changes, and adds the name of every named schema that changes to
     * {@code retypedSchemaNames}.
     */
    private Schema retypeSchema(Schema schema, PathNode node, String pathPrefix, Set<String> retypedSchemaNames) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema);
        boolean changed = false;
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            PathNode child = node.children.get(field.name());
            if (child == null || (child.target && field.schema().type() == Schema.Type.STRING)) {
                // A field that is already a string keeps its schema, including its logical name and parameters
                builder.field(field.name(), field.schema());
            }
            else if (child.target) {
                // A field that is itself a target is serialized whole; any deeper paths under it are
                // subsumed by the JSON string and ignored.
                builder.field(field.name(), field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                changed = true;
            }
            else {
                String path = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
                if (field.schema().type() != Schema.Type.STRUCT) {
                    throw new ConnectException("Field path '" + path + "' in '" + FIELDS_CONFIG
                            + "' navigates into a non-struct field of type " + field.schema().type()
                            + "; only struct fields can contain nested targets");
                }
                final Schema fieldSchema = retypeSchema(field.schema(), child, path, retypedSchemaNames);
                builder.field(field.name(), fieldSchema);
                changed |= fieldSchema != field.schema();
            }
        }
        if (!changed) {
            return schema;
        }
        if (schema.name() != null) {
            retypedSchemaNames.add(schema.name());
        }
        return builder.build();
    }

    /**
     * Fails when a retyped named schema occurs elsewhere in {@code schema} with a different definition, which happens
     * when a nested target is configured under only some of the fields that share that schema, such as the
     * {@code before} and {@code after} fields of a change-event envelope.
     */
    private void checkRetypedSchemaNames(Schema schema, Set<String> retypedSchemaNames, Map<String, Schema> schemasByName,
                                         Map<String, String> pathsByName, String pathPrefix) {
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            final Schema fieldSchema = field.schema();
            if (fieldSchema.type() != Schema.Type.STRUCT) {
                continue;
            }
            final String path = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
            if (retypedSchemaNames.contains(fieldSchema.name())) {
                final Schema firstSchema = schemasByName.putIfAbsent(fieldSchema.name(), fieldSchema);
                pathsByName.putIfAbsent(fieldSchema.name(), path);
                if (firstSchema != null && !firstSchema.equals(fieldSchema)) {
                    throw new ConnectException("The fields '" + pathsByName.get(fieldSchema.name()) + "' and '" + path
                            + "' share the schema '" + fieldSchema.name() + "', but '" + FIELDS_CONFIG
                            + "' retypes them differently; configure the same nested paths under both fields");
                }
            }
            checkRetypedSchemaNames(fieldSchema, retypedSchemaNames, schemasByName, pathsByName, path);
        }
    }

    /**
     * Builds the transformed value for {@code value} against {@code newSchema}, serializing targeted
     * fields and recursing into nested structs for deeper paths.
     */
    private Struct transformStruct(Struct value, Schema newSchema, PathNode node, String pathPrefix) {
        Struct newValue = new Struct(newSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            Object fieldValue = value.get(field);
            PathNode child = node.children.get(field.name());
            String path = pathPrefix.isEmpty() ? field.name() : pathPrefix + "." + field.name();
            if (child == null) {
                newValue.put(field.name(), fieldValue);
            }
            else if (child.target) {
                if (fieldValue != null && !(fieldValue instanceof String)) {
                    newValue.put(field.name(), toJsonString(path, field.schema(), fieldValue));
                }
                else {
                    newValue.put(field.name(), fieldValue);
                }
            }
            else if (fieldValue == null) {
                newValue.put(field.name(), null);
            }
            else {
                Schema childSchema = newSchema.field(field.name()).schema();
                newValue.put(field.name(), childSchema == field.schema() ? fieldValue
                        : transformStruct((Struct) fieldValue, childSchema, child, path));
            }
        }
        return newValue;
    }

    /**
     * Serializes a single field value to its JSON representation.
     */
    private String toJsonString(String fieldName, Schema fieldSchema, Object fieldValue) {
        try {
            byte[] json = jsonConverter.fromConnectData("_stringify", fieldSchema, fieldValue);
            return new String(json, StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            // A field that cannot be serialized must halt the stream rather than be dropped or
            // corrupted silently, so re-throw with enough context to identify the field, which leaves
            // the offset uncommitted. The value may hold row data, so it is only logged at TRACE.
            String schemaType = fieldSchema == null ? "null" : String.valueOf(fieldSchema.type());
            String schemaName = fieldSchema == null ? "null" : String.valueOf(fieldSchema.name());
            String valueClass = fieldValue == null ? "null" : fieldValue.getClass().getName();
            LOGGER.trace("Value of field '{}' that could not be serialized to a JSON string: {}", fieldName, fieldValue);
            throw new ConnectException("Could not serialize field '" + fieldName + "' (schema type=" + schemaType
                    + ", schema name=" + schemaName + ", value class=" + valueClass + ") to a JSON string", e);
        }
    }

    /**
     * Rejects field paths with an empty segment, such as {@code after.} or {@code after..payload}.
     */
    private static int validateFieldPaths(Configuration config, Field field, ValidationOutput problems) {
        if (!config.hasKey(field)) {
            return 0;
        }
        int errors = 0;
        for (String path : config.getList(field)) {
            if (Strings.isNullOrBlank(path)) {
                continue;
            }
            for (String segment : path.split("\\.", -1)) {
                if (Strings.isNullOrBlank(segment)) {
                    problems.accept(field, path, "Path segments must not be empty");
                    errors++;
                    break;
                }
            }
        }
        return errors;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, FIELDS_FIELD);
        return config;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(FIELDS_FIELD);
    }

    @Override
    public void close() {
        jsonConverter.close();
    }
}
