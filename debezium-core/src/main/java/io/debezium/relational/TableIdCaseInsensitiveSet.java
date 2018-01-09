/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * 
 * @author Jiri Pechanec
 *
 * A facade for Set implementation that accepts TableId and stores it with
 * table name converted to lower case.
 */
public class TableIdCaseInsensitiveSet implements Set<TableId> {

    public TableIdCaseInsensitiveSet(Set<TableId> delegate) {
        this.delegate = delegate;
    }

    private Set<TableId> delegate;

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(((TableId)o).toLowercase());
    }

    @Override
    public Iterator<TableId> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public boolean add(TableId e) {
        return delegate.add(((TableId)e).toLowercase());
    }

    @Override
    public boolean remove(Object o) {
        return delegate.remove(((TableId)o).toLowercase());
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends TableId> c) {
        return delegate.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return delegate.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public boolean equals(Object o) {
        return delegate.equals(o);
    }
}
