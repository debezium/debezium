/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.Test;

import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class ArraySerdesTest implements Testing {

    private static final ArraySerdes SERDES = new ArraySerdes();

    @Test
    public void shouldConvertFromBytesToArray1() throws IOException {
        readAsStringAndBytes("json/array1.json");
    }

    @Test
    public void shouldConvertFromBytesToArray2() throws IOException {
        readAsStringAndBytes("json/array2.json");
    }

    protected void readAsStringAndBytes(String resourceFile) throws IOException {
        String content = Testing.Files.readResourceAsString(resourceFile);
        Array doc = ArrayReader.defaultReader().readArray(content);
        byte[] bytes = SERDES.serialize("topicA", doc);
        Array reconstituted = SERDES.deserialize("topicA", bytes);
        assertThat((Object) reconstituted).isEqualTo(doc);
    }

}
