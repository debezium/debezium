/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

import org.junit.Test;

import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class DocumentSerdesTest implements Testing {

    private static final DocumentSerdes SERDES = new DocumentSerdes();

    @Test
    public void shouldConvertFromBytesToDocument1() throws IOException {
        readAsStringAndBytes("json/sample1.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument2() throws IOException {
        readAsStringAndBytes("json/sample2.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument3() throws IOException {
        readAsStringAndBytes("json/sample3.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocumentForResponse1() throws IOException {
        readAsStringAndBytes("json/response1.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocumentForResponse2() throws IOException {
        readAsStringAndBytes("json/response2.json");
    }

    protected void readAsStringAndBytes(String resourceFile) throws IOException {
        String content = Testing.Files.readResourceAsString(resourceFile);
        Document doc = DocumentReader.defaultReader().read(content);
        roundTrip(doc, size -> Testing.print("message size " + size + " bytes: \n" + doc));
    }

    protected void roundTrip(Document doc, IntConsumer sizeAccumulator) {
        byte[] bytes = SERDES.serialize("topicA", doc);
        if (sizeAccumulator != null) {
            sizeAccumulator.accept(bytes.length);
        }
        Document reconstituted = SERDES.deserialize("topicA", bytes);
        assertThat((Object) reconstituted).isEqualTo(doc);
    }

    protected List<Document> readResources(String prefix, String... resources) throws IOException {
        List<Document> documents = new ArrayList<>();
        for (String resource : resources) {
            String content = Testing.Files.readResourceAsString(prefix + resource);
            Array array = null;
            try {
                Document doc = DocumentReader.defaultReader().read(content);
                array = doc.getArray("entityChanges");
            }
            catch (IOException e) {
                array = ArrayReader.defaultReader().readArray(content);
            }
            array.forEach(entry -> documents.add(entry.getValue().asDocument()));
        }
        return documents;
    }

}
