/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import io.debezium.annotation.ThreadSafe;

/**
 * Writes {@link Document} instances to a variety of output forms.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public interface DocumentWriter {

    /**
     * Get the default DocumentWriter instance.
     *
     * @return the shared default writer instance; never null
     */
    static DocumentWriter defaultWriter() {
        return JacksonWriter.INSTANCE;
    }

    /**
     * Get the default DocumentWriter instance that outputs nicely-formatted JSON arrays.
     *
     * @return the shared default pretty writer instance; never null
     */
    static DocumentWriter prettyWriter() {
        return JacksonWriter.PRETTY_WRITER;
    }

    /**
     * Write the supplied array to bytes using UTF-8.
     * @param document the document to be written; may not be null
     * @return the bytes containing the output JSON-formatted document; never null
     */
    default byte[] writeAsBytes(Document document) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            write(document, stream);
            return stream.toByteArray();
        }
        catch (IOException e) {
            // This really should never happen ...
            e.printStackTrace();
            return new byte[]{};
        }
    }

    /**
     * Write the supplied document to bytes using UTF-8.
     * @param document the array to be written; may not be null
     * @param jsonStream the stream to which the document is to be written; may not be null
     * @throws IOException if a document could not be written to the supplied stream
     */
    void write(Document document, OutputStream jsonStream) throws IOException;

    /**
     * Write the supplied document to bytes using UTF-8.
     * @param document the array to be written; may not be null
     * @param jsonWriter the writer to which the document is to be written; may not be null
     * @throws IOException if a document could not be written to the supplied stream
     */
    void write(Document document, Writer jsonWriter) throws IOException;

    /**
     * Write the supplied document to a string using UTF-8.
     * @param document the document to be written; may not be null
     * @return the string containing the output JSON-formatted document; never null
     * @throws IOException if a document could not be written to the supplied stream
     */
    String write(Document document) throws IOException;

}
