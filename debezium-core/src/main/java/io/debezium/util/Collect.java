/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * A set of utilities for more easily creating various kinds of collections.
 */
public class Collect {

    /**
     * Create a fixed sized Map that removes the least-recently used entry when the map becomes too large. The supplied
     * {@code maximumNumberOfEntries} should be a power of 2 to efficiently make efficient use of memory. If not, the resulting
     * map will be able to contain no more than {@code maximumNumberOfEntries} entries, but the underlying map will have a
     * capacity that is the next power of larger than the supplied {@code maximumNumberOfEntries} value so that it can hold
     * the required number of entries.
     *
     * @param maximumNumberOfEntries the maximum number of entries allowed in the map; should be a power of 2
     * @return the map that is limited in size by the specified number of entries; never null
     */
    public static <K, V> Map<K, V> fixedSizeMap(int maximumNumberOfEntries) {
        return new LinkedHashMap<K, V>(maximumNumberOfEntries + 1, .75F, true) { // throws illegal argument if < 0
            private static final long serialVersionUID = 1L;
            final int evictionSize = maximumNumberOfEntries - 1;

            @Override
            public boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > evictionSize;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T, V> Set<T> unmodifiableSet(Function<V, T> extractor, V... values) {
        Set<T> newSet = new HashSet<>();
        for (V value : values) {
            if (value != null) {
                newSet.add(extractor.apply(value));
            }
        }
        return Collections.unmodifiableSet(newSet);
    }

    public static <T, V> Set<T> unmodifiableSet(Function<V, T> extractor, Collection<V> values) {
        Set<T> newSet = new HashSet<>();
        for (V value : values) {
            if (value != null) {
                newSet.add(extractor.apply(value));
            }
        }
        return Collections.unmodifiableSet(newSet);
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> unmodifiableSet(Set<T> values, T... additionalValues) {
        Set<T> newSet = new HashSet<>(values);
        for (T value : values) {
            if (value != null) {
                newSet.add(value);
            }
        }
        return Collections.unmodifiableSet(newSet);
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> unmodifiableSet(T... values) {
        return unmodifiableSet(arrayListOf(values));
    }

    public static <T> Set<T> unmodifiableSet(Collection<T> values) {
        return Collections.unmodifiableSet(new HashSet<T>(values));
    }

    public static <T> Set<T> unmodifiableSet(Set<T> values) {
        return Collections.unmodifiableSet(values);
    }

    public static <T> Set<T> unmodifiableSet(Iterator<T> values) {
        Set<T> set = new HashSet<>();
        while (values.hasNext()) {
            set.add(values.next());
        }

        return Collections.unmodifiableSet(set);
    }

    public static <T> List<T> arrayListOf(T[] values) {
        List<T> result = new ArrayList<>();
        for (T value : values) {
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> arrayListOf(T first, T... additional) {
        List<T> result = new ArrayList<>();
        result.add(first);
        for (T another : additional) {
            result.add(another);
        }
        return result;
    }

    public static <T> List<T> arrayListOf(Iterable<T> values) {
        List<T> result = new ArrayList<>();
        values.forEach((value) -> result.add(value));
        return result;
    }

    public static <K, V> Map<K, V> mapOf(K key, V value) {
        return Collections.singletonMap(key, value);
    }

    public static <K, V> Map<K, V> hashMapOf(K key, V value) {
        Map<K, V> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static <K, V> Map<K, V> hashMapOf(K key1, V value1, K key2, V value2) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    public static <K, V> Map<K, V> hashMapOf(K key1, V value1, K key2, V value2, K key3, V value3) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    public static <K, V> Map<K, V> hashMapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        return map;
    }

    public static <K, V> Map<K, V> hashMapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        map.put(key5, value5);
        return map;
    }

    public static <K, V> Map<K, V> hashMapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5, K key6, V value6) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        map.put(key5, value5);
        map.put(key6, value6);
        return map;
    }

    public static <K, V> Map<K, V> linkMapOf(K key, V value) {
        Map<K, V> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    public static <K, V> Map<K, V> linkMapOf(K key1, V value1, K key2, V value2) {
        Map<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    public static <K, V> Map<K, V> linkMapOf(K key1, V value1, K key2, V value2, K key3, V value3) {
        Map<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    public static <K, V> Map<K, V> linkMapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        Map<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        return map;
    }

    public static Properties propertiesOf(String key, String value) {
        Properties props = new Properties();
        props.put(key, value);
        return props;
    }

    public static Properties propertiesOf(String key1, String value1, String key2, String value2) {
        Properties props = new Properties();
        props.put(key1, value1);
        props.put(key2, value2);
        return props;
    }

    public static Properties propertiesOf(String key1, String value1, String key2, String value2, String key3, String value3) {
        Properties props = new Properties();
        props.put(key1, value1);
        props.put(key2, value2);
        props.put(key3, value3);
        return props;
    }

    public static Properties propertiesOf(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4) {
        Properties props = new Properties();
        props.put(key1, value1);
        props.put(key2, value2);
        props.put(key3, value3);
        props.put(key4, value4);
        return props;
    }

    public static Properties propertiesOf(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5,
                                          String value5) {
        Properties props = new Properties();
        props.put(key1, value1);
        props.put(key2, value2);
        props.put(key3, value3);
        props.put(key4, value4);
        props.put(key5, value5);
        return props;
    }

    public static Properties propertiesOf(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5,
                                          String value5, String key6, String value6) {
        Properties props = new Properties();
        props.put(key1, value1);
        props.put(key2, value2);
        props.put(key3, value3);
        props.put(key4, value4);
        props.put(key5, value5);
        props.put(key6, value6);
        return props;
    }

    /**
     * Set the value at the given position in the list, expanding the list as required to accommodate the new position.
     * <p>
     * This is not a thread-safe operation
     *
     * @param list the list to be modified
     * @param index the index position of the new value
     * @param value the value
     * @param defaultValue the value used for intermediate positions when expanding the list; may be null
     */
    public static <T> void set(List<T> list, int index, T value, T defaultValue) {
        while (list.size() <= index) {
            list.add(defaultValue);
        }
        list.set(index, value);
    }

    /**
     * Remove the content of one set from an another one.
     *
     * @param subtrahend the main set 
     * @param minuend the elements to be removed
     */
    public static <T> Set<T> minus(Set<T> subtrahend, Set<T> minuend) {
        final Set<T> r = new HashSet<T>(subtrahend);
        r.removeAll(minuend);
        return r;
    }

    private Collect() {
    }
}
