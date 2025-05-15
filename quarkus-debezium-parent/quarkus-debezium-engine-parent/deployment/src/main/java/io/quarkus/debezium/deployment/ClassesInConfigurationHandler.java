/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class ClassesInConfigurationHandler {

    private static final String DELIMITER = ",";
    private final String type;

    ClassesInConfigurationHandler(String type) {
        this.type = type;
    }

    List<String> extract(Map<String, String> config) {
        String elements = config.get(type);

        if (elements == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(elements.split(DELIMITER))
                .map(value -> config.get(type + "." + value + ".type"))
                .toList();
    }

    public static ClassesInConfigurationHandler TRANSFORM = new ClassesInConfigurationHandler("transforms");

    public static ClassesInConfigurationHandler PREDICATE = new ClassesInConfigurationHandler("predicates");
}
