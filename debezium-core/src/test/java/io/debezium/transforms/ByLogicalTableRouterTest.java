/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author Mario Mueller
 */
public class ByLogicalTableRouterTest {

    @Test(expected = ConnectException.class)
    public void testBrokenKeyReplacementConfigurationNullValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "If this is set, key.field.replacement must be non-empty");
        subject.configure(props);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenKeyReplacementConfigurationEmptyValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "If this is set, key.field.replacement must be non-empty");
        props.put("key.field.replacement", "");
        subject.configure(props);
    }

    @Test
    public void testKeyReplacementWorkingConfiguration() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "$1");
        props.put("key.field.regex", "anotherValidRegex(.*)");
        props.put("key.field.replacement", "$1");
        subject.configure(props);
        assertTrue(true);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenTopicReplacementConfigurationNullValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        // topic.replacement is not set, therefore null. Must crash.
        props.put("topic.regex", "someValidRegex(.*)");
        subject.configure(props);
    }

    @Test(expected = ConnectException.class)
    public void testBrokenTopicReplacementConfigurationEmptyValue() {
        final ByLogicalTableRouter<SourceRecord> subject = new ByLogicalTableRouter<>();
        final Map<String, String> props = new HashMap<>();

        // topic.replacement is set to empty string. Must crash.
        props.put("topic.regex", "someValidRegex(.*)");
        props.put("topic.replacement", "");
        subject.configure(props);
    }

    // FIXME: This SMT can use more tests for more detailed coverage.
    // The creation of a DBZ-ish SourceRecord is required for each test
}
