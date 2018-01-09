/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Jiri Pechanec
 *
 * A facade for Map implementation that accepts TableId as key and stores the key with
 * table name converted to lower case.
 */
public class TableIdCaseInsensitiveMap<V> implements Map<TableId, V> {

    private Map<TableId, V> delegate;

    public TableIdCaseInsensitiveMap(Map<TableId, V> map) {
        delegate = map;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(((TableId)key).toLowercase());
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return delegate.get(((TableId)key).toLowercase());
    }

    @Override
    public V put(TableId key, V value) {
        return delegate.put(((TableId)key).toLowercase(), value);
    }

    @Override
    public V remove(Object key) {
        return delegate.remove(((TableId)key).toLowercase());
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<TableId> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<V> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<TableId, V>> entrySet() {
        return delegate.entrySet();
    }

    @Override
    public void putAll(Map<? extends TableId, ? extends V> m) {
        m.entrySet().stream().forEach(x -> put(((TableId)x.getKey()).toLowercase(), x.getValue()));
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public boolean equals(Object o) {
        return delegate.equals(o);
    }
}
