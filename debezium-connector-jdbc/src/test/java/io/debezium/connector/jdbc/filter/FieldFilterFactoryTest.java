/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.debezium.sink.filter.FieldFilterFactory;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;

public class FieldFilterFactoryTest {

    @Test
    public void testFullyQualifiedFilter() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter("mytopic:myfield, mytopic2:myfield2", null);
        assertTrue(filter.matches("mytopic", "myfield"), "matchees first item");
        assertTrue(filter.matches("mytopic2", "myfield2"), "matchees second item");
        assertFalse(filter.matches("mytopic3", "myfield"), "doesnt match when topic is different");
        assertFalse(filter.matches("mytopic", "myfield3"), "doesnt match when field is different");
    }

    @Test
    public void testUnqualifiedFilter() {
        FieldNameFilter filter = FieldFilterFactory.createFieldFilter("myfield, myfield2", null);
        assertTrue(filter.matches("mytopic", "myfield"), "matchees first item");
        assertTrue(filter.matches("mytopic3", "myfield"), "matches first field in another topic");
        assertTrue(filter.matches("mytopic2", "myfield2"), "matchees second item");
        assertFalse(filter.matches("mytopic", "myfield3"), "doesnt match when field is different");
    }

}
