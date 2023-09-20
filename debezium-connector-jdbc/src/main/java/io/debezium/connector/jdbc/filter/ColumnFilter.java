/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A generalized {@link ColumnFilter} implementation.
 *
 * @author Anisha Mohanty
 */
public class ColumnFilter {

    public static Map<String, List<String>> parse(String list) {
        if (list == null) {
            return new HashMap<>();
        }
        Map<String, List<String>> map = new HashMap<>();
        String[] topicColumnPairs = list.split(",");
        for (String topicColumnPair : topicColumnPairs) {
            String[] parts = topicColumnPair.split(":");
            if (parts.length == 2) {
                String topic = parts[0].trim();
                String column = parts[1].trim();

                if (!map.containsKey(topic)) {
                    map.put(topic, new ArrayList<>());
                }

                map.get(topic).add(column);
            }
        }
        return map;
    }

    public static boolean isColumnIncluded(String topic, String column, Map<String, List<String>> map) {
        List<String> columns = map.get(topic);
        return columns.contains(column);
    }

    public static boolean isColumnExcluded(String topic, String column, Map<String, List<String>> map) {
        List<String> columns = map.get(topic);
        return columns.contains(column);
    }
}
