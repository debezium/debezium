/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import org.junit.Before;

import io.debezium.transforms.extractnewstate.DefaultDeleteHandlingStrategy;
import io.debezium.util.Collect;

/**
 * Unit test for {@link ExtractNewDocumentState} and {@link DefaultDeleteHandlingStrategy}.
 *
 * @author Harvey Yue
 */
public class NewExtractNewDocumentStateTest extends ExtractNewDocumentStateTest {
    @Before
    public void setup() {
        transformation = new ExtractNewDocumentState<>();
        transformation.configure(Collect.hashMapOf(
                "array.encoding", "array",
                "delete.tombstone.handling.mode", "tombstone"));
    }
}
