/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class DocumentTest {
    
    private Document doc;
    private Map<Path,Value> found = new LinkedHashMap<>();
    private Iterator<Map.Entry<Path, Value>> iterator;
    
    @Before
    public void beforeEach() {
        doc = null;
        found = new LinkedHashMap<>();
        iterator = null;
    }
    
    @Test
    public void shouldPerformForEachOnFlatDocument() {
        doc = Document.create("a","A","b","B");
        doc.forEach((path,value)->found.put(path,value));
        iterator = found.entrySet().iterator();
        assertPair(iterator,"/a","A");
        assertPair(iterator,"/b","B");
        assertNoMore(iterator);
    }
    
    protected void assertPair( Iterator<Map.Entry<Path, Value>> iterator, String path, Object value ) {
        Map.Entry<Path,Value> entry = iterator.next();
        assertThat((Object)entry.getKey()).isEqualTo(Path.parse(path));
        assertThat(entry.getValue()).isEqualTo(Value.create(value));
    }
    
    protected void assertNoMore( Iterator<Map.Entry<Path, Value>> iterator ) {
        assertThat(iterator.hasNext()).isFalse();
    }
    
}
