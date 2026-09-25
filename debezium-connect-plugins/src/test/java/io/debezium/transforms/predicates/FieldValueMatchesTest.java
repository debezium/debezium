/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.predicates;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link FieldValueMatches} predicate, which matches a record when a value field,
 * addressed by a dot path, matches a regular expression.
 */
class FieldValueMatchesTest {

    private final FieldValueMatches<SourceRecord> predicate = new FieldValueMatches<>();

    @AfterEach
    void close() {
        predicate.close();
    }

    private SourceRecord recordWith(Schema valueSchema, Object value) {
        return new SourceRecord(null, null, "topic", 0, null, null, valueSchema, value);
    }

    /** Value schema: id INT32, status STRING, after STRUCT{status STRING}. */
    private Schema valueSchema() {
        Schema after = SchemaBuilder.struct().name("After")
                .field("status", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        return SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("status", Schema.OPTIONAL_STRING_SCHEMA)
                .field("after", after)
                .build();
    }

    private Struct valueWith(Integer id, String status, String afterStatus) {
        Schema schema = valueSchema();
        Struct after = new Struct(schema.field("after").schema()).put("status", afterStatus);
        return new Struct(schema).put("id", id).put("status", status).put("after", after);
    }

    @Test
    void matchesTopLevelFieldAgainstPattern() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status", FieldValueMatches.PATTERN_CONFIG, "active"));
        Struct value = valueWith(1, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isTrue();
    }

    @Test
    void doesNotMatchWhenPatternDiffers() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status", FieldValueMatches.PATTERN_CONFIG, "active"));
        Struct value = valueWith(1, "archived", "archived");
        assertThat(predicate.test(recordWith(value.schema(), value))).isFalse();
    }

    @Test
    void appliesFullMatchByDefaultNotPartialFind() {
        // The default match.mode=full anchors the whole value, so a substring pattern does not match.
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status", FieldValueMatches.PATTERN_CONFIG, "act"));
        Struct value = valueWith(1, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isFalse();
    }

    @Test
    void matchesSubstringWhenMatchModeIsPartial() {
        // match.mode=partial uses Matcher.find(), so a substring pattern matches.
        predicate.configure(Map.of(
                FieldValueMatches.FIELD_CONFIG, "status",
                FieldValueMatches.PATTERN_CONFIG, "act",
                FieldValueMatches.MATCH_MODE_CONFIG, FieldValueMatches.MATCH_MODE_PARTIAL));
        Struct value = valueWith(1, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isTrue();
    }

    @Test
    void rejectsInvalidMatchModeAtConfigureTime() {
        assertThatThrownBy(() -> predicate.configure(Map.of(
                FieldValueMatches.FIELD_CONFIG, "status",
                FieldValueMatches.PATTERN_CONFIG, ".*",
                FieldValueMatches.MATCH_MODE_CONFIG, "bogus")))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void matchesNestedFieldByDotPath() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "after.status", FieldValueMatches.PATTERN_CONFIG, "active"));
        Struct value = valueWith(1, "archived", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isTrue();
    }

    @Test
    void matchesNonStringScalarViaItsStringForm() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "id", FieldValueMatches.PATTERN_CONFIG, "\\d+"));
        Struct value = valueWith(42, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isTrue();
    }

    @Test
    void treatsAbsentFieldAsNotMatchingByDefault() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "missing", FieldValueMatches.PATTERN_CONFIG, ".*"));
        Struct value = valueWith(1, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isFalse();
    }

    @Test
    void failsOnAbsentFieldWhenConfiguredToFail() {
        predicate.configure(Map.of(
                FieldValueMatches.FIELD_CONFIG, "missing",
                FieldValueMatches.PATTERN_CONFIG, ".*",
                FieldValueMatches.UNEVALUABLE_VALUE_CONFIG, FieldValueMatches.UNEVALUABLE_FAIL));
        Struct value = valueWith(1, "active", "active");
        assertThatThrownBy(() -> predicate.test(recordWith(value.schema(), value)))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void treatsNullFieldValueAsNotMatchingByDefault() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status", FieldValueMatches.PATTERN_CONFIG, ".*"));
        Struct value = valueWith(1, null, "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isFalse();
    }

    @Test
    void treatsNonStructRecordValueAsNotMatchingByDefault() {
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status", FieldValueMatches.PATTERN_CONFIG, ".*"));
        assertThat(predicate.test(recordWith(null, null))).isFalse();
    }

    @Test
    void treatsNonScalarTerminalFieldAsNotMatchingByDefault() {
        // 'after' resolves to a Struct, which a regex cannot meaningfully match.
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "after", FieldValueMatches.PATTERN_CONFIG, ".*"));
        Struct value = valueWith(1, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isFalse();
    }

    @Test
    void treatsNonStructIntermediateSegmentAsNotMatchingByDefault() {
        // 'status' is a scalar, so navigating 'status.foo' cannot descend into it.
        predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status.foo", FieldValueMatches.PATTERN_CONFIG, ".*"));
        Struct value = valueWith(1, "active", "active");
        assertThat(predicate.test(recordWith(value.schema(), value))).isFalse();
    }

    @Test
    void rejectsMissingRequiredFieldAtConfigureTime() {
        assertThatThrownBy(() -> predicate.configure(Map.of(FieldValueMatches.PATTERN_CONFIG, ".*")))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void rejectsMissingRequiredPatternAtConfigureTime() {
        assertThatThrownBy(() -> predicate.configure(Map.of(FieldValueMatches.FIELD_CONFIG, "status")))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void rejectsInvalidRegexAtConfigureTime() {
        assertThatThrownBy(() -> predicate.configure(Map.of(
                FieldValueMatches.FIELD_CONFIG, "status",
                FieldValueMatches.PATTERN_CONFIG, "[unclosed")))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void rejectsInvalidUnevaluableValueAtConfigureTime() {
        assertThatThrownBy(() -> predicate.configure(Map.of(
                FieldValueMatches.FIELD_CONFIG, "status",
                FieldValueMatches.PATTERN_CONFIG, ".*",
                FieldValueMatches.UNEVALUABLE_VALUE_CONFIG, "bogus")))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void rejectsEmptyPathSegmentAtConfigureTime() {
        assertThatThrownBy(() -> predicate.configure(Map.of(
                FieldValueMatches.FIELD_CONFIG, "after..status",
                FieldValueMatches.PATTERN_CONFIG, ".*")))
                .isInstanceOf(ConnectException.class);
    }
}
