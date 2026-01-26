/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LinkedHashMapExtractor {

    public static <K, V> List<V> extractFirstEntries(LinkedHashMap<K, V> map, int count) {
        ArrayList<V> result = new ArrayList<>();

        Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();

        int extracted = 0;
        while (iterator.hasNext() && extracted < count) {
            Map.Entry<K, V> entry = iterator.next();
            result.add(entry.getValue());
            iterator.remove();
            extracted++;
        }

        return result;
    }
}
