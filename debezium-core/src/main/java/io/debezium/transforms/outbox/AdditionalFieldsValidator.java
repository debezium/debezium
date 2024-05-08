/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Validator for additional fields in outbox event router
 *
 * @author Sungho Hwang
 */
public class AdditionalFieldsValidator {
    public static int isListOfStringPairs(Configuration config, Field field, Field.ValidationOutput problems) {
        List<String> value = config.getStrings(field, ",");
        int errors = 0;

        if (value == null) {
            return errors;
        }

        for (String mapping : value) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2 && parts.length != 3) {
                problems.accept(field, value, "A comma-separated list of valid String pairs or trios " +
                        "is expected but got: " + value);
                ++errors;
            }
        }
        return errors;
    }
}
