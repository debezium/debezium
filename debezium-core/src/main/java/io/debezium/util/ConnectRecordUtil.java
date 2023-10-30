/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.InsertField;

/**
 * A set of utilities for more easily creating various kinds of transformations.
 */
public class ConnectRecordUtil {

    private static final String UPDATE_DESCRIPTION = "updateDescription";

    public static <R extends ConnectRecord<R>> ExtractField<R> extractAfterDelegate() {
        return extractValueDelegate(AFTER);
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractBeforeDelegate() {
        return extractValueDelegate(BEFORE);
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractUpdateDescriptionDelegate() {
        return extractValueDelegate(UPDATE_DESCRIPTION);
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractValueDelegate(String field) {
        ExtractField<R> extractField = new ExtractField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", field);
        extractField.configure(delegateConfig);
        return extractField;
    }

    public static <R extends ConnectRecord<R>> ExtractField<R> extractKeyDelegate(String field) {
        ExtractField<R> extractField = new ExtractField.Key<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", field);
        extractField.configure(delegateConfig);
        return extractField;
    }

    public static <R extends ConnectRecord<R>> InsertField<R> insertStaticValueDelegate(String field, String value) {
        InsertField<R> insertDelegate = new InsertField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", field);
        delegateConfig.put("static.value", value);
        insertDelegate.configure(delegateConfig);
        return insertDelegate;
    }

    public static <R extends ConnectRecord<R>> Flatten<R> flattenValueDelegate(String delimiter) {
        Flatten<R> recordFlattener = new Flatten.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("delimiter", delimiter);
        recordFlattener.configure(delegateConfig);
        return recordFlattener;
    }
}
