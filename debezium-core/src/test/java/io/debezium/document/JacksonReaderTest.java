/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link JacksonReader}.
 *
 * @author Gunnar Morling
 */
public class JacksonReaderTest {

    @Test
    @FixFor("DBZ-657")
    public void canParseDocumentWithUnescapedControlCharacter() throws Exception {
        Document document = JacksonReader.DEFAULT_INSTANCE.read(
                // { " a CR b " : 1 2 3 }
                new String(new byte[]{ 123, 34, 97, 13, 98, 34, 58, 49, 50, 51, 125 }));

        assertThat((Object) document).isEqualTo(Document.create("a\rb", 123));
    }
}
