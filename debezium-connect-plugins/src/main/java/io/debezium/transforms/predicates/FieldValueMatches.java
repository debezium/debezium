/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.predicates;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.transforms.SmtManager;
import io.debezium.util.Strings;

/**
 * A Kafka Connect {@link org.apache.kafka.connect.transforms.predicates.Predicate} that matches a
 * record when a value field, addressed by a dot path, matches a regular expression.
 * <p>
 * Debezium Server maps a single source to a single sink, so filtering there is global in scope and a
 * sink-specific filter surface adds a second, overlapping way to express what the transform chain
 * already covers. Operation filtering is handled by {@code skipped.operations} (and
 * {@code snapshot.mode=no_data} for read events); topic and header matching are handled by the stock
 * {@code TopicNameMatches} and {@code HasHeaderKey} predicates. The one gap those leave is matching on
 * a <em>value</em> without reaching for scripting. This predicate fills that gap: it is
 * Kafka-Connect-compatible and composes with the stock {@code Filter} transformation, so no new sink
 * configuration namespace is introduced.
 * <p>
 * Configuration:
 * <ul>
 * <li>{@code field} &mdash; the value field to test, addressed with dot notation. A bare name such as
 * {@code status} targets a top-level field; a dotted name such as {@code after.status} descends into
 * nested structs, for example to reach into a change-event envelope. Every segment except the last
 * must resolve to a {@code STRUCT}.</li>
 * <li>{@code pattern} &mdash; a Java regular expression tested against the field's string value.</li>
 * <li>{@code match.mode} &mdash; how the pattern is applied: {@code full} (the default) requires the
 * whole value to match, as with {@link java.util.regex.Matcher#matches()}; {@code partial} is
 * satisfied when the pattern matches anywhere in the value, as with
 * {@link java.util.regex.Matcher#find()}.</li>
 * <li>{@code unevaluable.value} &mdash; the outcome when the pattern cannot be evaluated against the
 * field: the record value is not a struct, the path is absent, an intermediate segment is not a
 * struct, the field is {@code null}, or the field is not a scalar. {@code ignore} (the default) treats
 * such a record as not matching; {@code fail} halts the stream with a {@link ConnectException} so that
 * a misconfiguration or an unexpected shape is not masked.</li>
 * </ul>
 * <p>
 * The predicate matches against the field value as it stands in the record's {@code Struct} (the
 * substrate the Connect predicate contract exposes), not against a serialized-JSON rendering of the
 * value.
 *
 * @param <R> the type of {@link ConnectRecord} the predicate applies to
 */
public class FieldValueMatches<R extends ConnectRecord<R>> implements org.apache.kafka.connect.transforms.predicates.Predicate<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldValueMatches.class);

    public static final String FIELD_CONFIG = "field";
    public static final String PATTERN_CONFIG = "pattern";
    public static final String MATCH_MODE_CONFIG = "match.mode";
    public static final String UNEVALUABLE_VALUE_CONFIG = "unevaluable.value";

    public static final String MATCH_MODE_FULL = "full";
    public static final String MATCH_MODE_PARTIAL = "partial";

    public static final String UNEVALUABLE_IGNORE = "ignore";
    public static final String UNEVALUABLE_FAIL = "fail";

    private static final Field FIELD_FIELD = Field.create(FIELD_CONFIG)
            .withDisplayName("Value field to match")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("The value field to test, addressed with dot notation. A bare name targets a "
                    + "top-level field; a dotted name such as 'after.status' descends into nested structs.");

    private static final Field PATTERN_FIELD = Field.create(PATTERN_CONFIG)
            .withDisplayName("Match pattern")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("A Java regular expression tested against the field's string value.");

    private static final Field MATCH_MODE_FIELD = Field.create(MATCH_MODE_CONFIG)
            .withDisplayName("Pattern match mode")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(MATCH_MODE_FULL)
            .withDescription("How the pattern is applied: 'full' requires the whole value to match, "
                    + "'partial' matches when the pattern is found anywhere in the value.");

    private static final Field UNEVALUABLE_VALUE_FIELD = Field.create(UNEVALUABLE_VALUE_CONFIG)
            .withDisplayName("Behavior when the value cannot be evaluated")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(UNEVALUABLE_IGNORE)
            .withDescription("What to do when the pattern cannot be evaluated against the field (value not a "
                    + "struct, path absent, intermediate not a struct, field null, or field not scalar): "
                    + "'ignore' treats the record as not matching, 'fail' halts the stream.");

    private String fieldPath;
    private String[] pathSegments;
    private Pattern pattern;
    private boolean fullMatch;
    private boolean failOnUnevaluable;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELD_FIELD, PATTERN_FIELD, MATCH_MODE_FIELD, UNEVALUABLE_VALUE_FIELD));

        this.fieldPath = config.getString(FIELD_FIELD);
        this.pathSegments = splitPath(fieldPath);

        final String regex = config.getString(PATTERN_FIELD);
        try {
            this.pattern = Pattern.compile(regex);
        }
        catch (PatternSyntaxException e) {
            throw new ConnectException("Invalid regular expression '" + regex + "' in '" + PATTERN_CONFIG + "'", e);
        }

        final String matchMode = config.getString(MATCH_MODE_FIELD).trim().toLowerCase();
        switch (matchMode) {
            case MATCH_MODE_FULL -> this.fullMatch = true;
            case MATCH_MODE_PARTIAL -> this.fullMatch = false;
            default -> throw new ConnectException("Invalid value '" + matchMode + "' for '" + MATCH_MODE_CONFIG
                    + "'; expected '" + MATCH_MODE_FULL + "' or '" + MATCH_MODE_PARTIAL + "'");
        }

        final String unevaluable = config.getString(UNEVALUABLE_VALUE_FIELD).trim().toLowerCase();
        switch (unevaluable) {
            case UNEVALUABLE_IGNORE -> this.failOnUnevaluable = false;
            case UNEVALUABLE_FAIL -> this.failOnUnevaluable = true;
            default -> throw new ConnectException("Invalid value '" + unevaluable + "' for '" + UNEVALUABLE_VALUE_CONFIG
                    + "'; expected '" + UNEVALUABLE_IGNORE + "' or '" + UNEVALUABLE_FAIL + "'");
        }
    }

    private String[] splitPath(String path) {
        final String[] segments = path.split("\\.", -1);
        for (final String segment : segments) {
            if (Strings.isNullOrBlank(segment)) {
                throw new ConnectException("Invalid field path '" + path + "' in '" + FIELD_CONFIG
                        + "': path segments must not be empty");
            }
        }
        return segments;
    }

    @Override
    public boolean test(R record) {
        final Object value = record.value();
        if (!(value instanceof Struct)) {
            return onUnevaluable(record, "the record value is not a struct");
        }

        Struct current = (Struct) value;
        for (int i = 0; i < pathSegments.length; i++) {
            final String segment = pathSegments[i];
            if (current.schema().field(segment) == null) {
                return onUnevaluable(record, "field '" + segment + "' is absent on the path '" + fieldPath + "'");
            }
            final Object segmentValue = current.get(segment);
            final boolean last = i == pathSegments.length - 1;
            if (last) {
                return matchTerminal(record, segmentValue);
            }
            if (!(segmentValue instanceof Struct)) {
                return onUnevaluable(record, "path segment '" + segment + "' on '" + fieldPath + "' is not a struct");
            }
            current = (Struct) segmentValue;
        }
        // pathSegments is never empty, so the loop always returns; this satisfies the compiler.
        return onUnevaluable(record, "empty field path");
    }

    private boolean matchTerminal(R record, Object fieldValue) {
        if (fieldValue == null) {
            return onUnevaluable(record, "field '" + fieldPath + "' is null");
        }
        if (fieldValue instanceof Struct || fieldValue instanceof Map || fieldValue instanceof List) {
            return onUnevaluable(record, "field '" + fieldPath + "' is not a scalar and cannot be matched by a pattern");
        }
        final var matcher = pattern.matcher(String.valueOf(fieldValue));
        return fullMatch ? matcher.matches() : matcher.find();
    }

    private boolean onUnevaluable(R record, String reason) {
        if (failOnUnevaluable) {
            LOGGER.error("Cannot evaluate predicate on field '{}': {}. The stream halts because '{}' is '{}'.",
                    fieldPath, reason, UNEVALUABLE_VALUE_CONFIG, UNEVALUABLE_FAIL);
            throw new ConnectException("Cannot evaluate predicate on field '" + fieldPath + "': " + reason);
        }
        LOGGER.debug("Predicate on field '{}' is not satisfied: {}", fieldPath, reason);
        return false;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, FIELD_FIELD, PATTERN_FIELD, MATCH_MODE_FIELD, UNEVALUABLE_VALUE_FIELD);
        return config;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(FIELD_FIELD, PATTERN_FIELD, MATCH_MODE_FIELD, UNEVALUABLE_VALUE_FIELD);
    }

    @Override
    public void close() {
    }
}
