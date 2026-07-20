/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class CustomMetricTagsValidationTest {

    private List<String> validationProblems(String rawValue) {
        Configuration.Builder builder = Configuration.create();
        if (rawValue != null) {
            builder.with(CommonConnectorConfig.CUSTOM_METRIC_TAGS, rawValue);
        }

        List<String> problems = new ArrayList<>();
        CommonConnectorConfig.CUSTOM_METRIC_TAGS.validate(builder.build(),
                (field, value, message) -> problems.add(message));
        return problems;
    }

    @Test
    void shouldRejectKeysWithInvalidJmxSyntax() {
        assertThat(validationProblems("region:eu=prod")).anySatisfy(
                message -> assertThat(message).contains("not a valid JMX ObjectName"));
        assertThat(validationProblems("a*b=v")).anySatisfy(
                message -> assertThat(message).contains("not a valid JMX ObjectName"));
        assertThat(validationProblems("a?b=v")).anySatisfy(
                message -> assertThat(message).contains("not a valid JMX ObjectName"));
    }

    @Test
    void shouldAcceptWellFormedTagsAndSanitizableValues() {
        assertThat(validationProblems("region=eu,team=data")).isEmpty();
        assertThat(validationProblems("region=eu:west")).isEmpty();
        assertThat(validationProblems("database=salesdb-streaming,table=inventory")).isEmpty();
    }

    @Test
    void shouldAcceptBlankOrUnsetTags() {
        assertThat(validationProblems("")).isEmpty();
        assertThat(validationProblems(null)).isEmpty();
    }

    @Test
    void shouldRejectStructurallyMalformedTags() {
        assertThat(validationProblems("foo")).isNotEmpty();
        assertThat(validationProblems("k1=a=b")).isNotEmpty();
    }
}
