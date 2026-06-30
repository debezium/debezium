/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.filter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;

class FieldFilterFactoryTest {

    @FixFor("debezium/dbz#1185")
    @Test
    void defaultFilterShouldMatchEverything() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter(null, null);

        assertThat(filter.matches("any.topic", "any_field")).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void includeFilterShouldMatchSpecifiedFields() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter("name,age", null);

        assertThat(filter.matches("any.topic", "name")).isTrue();
        assertThat(filter.matches("any.topic", "age")).isTrue();
        assertThat(filter.matches("any.topic", "email")).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void excludeFilterShouldRejectSpecifiedFields() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter(null, "password,secret");

        assertThat(filter.matches("any.topic", "name")).isTrue();
        assertThat(filter.matches("any.topic", "password")).isFalse();
        assertThat(filter.matches("any.topic", "secret")).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void excludeFilterTakesPrecedenceOverInclude() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter("name", "password");

        assertThat(filter.matches("any.topic", "name")).isTrue();
        assertThat(filter.matches("any.topic", "password")).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void includeFilterWithTopicQualification() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter("my.topic:name,my.topic:age", null);

        assertThat(filter.matches("my.topic", "name")).isTrue();
        assertThat(filter.matches("my.topic", "age")).isTrue();
        assertThat(filter.matches("other.topic", "name")).isFalse();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void excludeFilterWithTopicQualification() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter(null, "my.topic:secret");

        assertThat(filter.matches("my.topic", "secret")).isFalse();
        assertThat(filter.matches("my.topic", "name")).isTrue();
        assertThat(filter.matches("other.topic", "secret")).isTrue();
    }

    @FixFor("debezium/dbz#1185")
    @Test
    void emptyStringsShouldReturnDefaultFilter() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter("", "");

        assertThat(filter.matches("any.topic", "any_field")).isTrue();
    }
}
