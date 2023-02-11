/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link BinaryValue}.
 *
 * @author Gunnar Morling
 */
public class BinaryValueTest {

    @Test
    @FixFor("DBZ-759")
    public void equalsAndHashCode() {
        Value binaryValue1 = Value.create(new byte[]{ 1, 2, 3 });
        Value binaryValue2 = Value.create(new byte[]{ 1, 2, 3 });

        Set<Value> set = Collections.singleton(binaryValue1);

        assertThat(set).contains(binaryValue2);
    }
}
