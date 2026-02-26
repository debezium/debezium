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
 * Writes {@link Array} instances to a variety of output forms.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public interface ArrayWriter {

    /**
     * Get the default ArrayWriter instance.
     * @return the shared default writer instance; never null
     */
    static ArrayWriter defaultWriter() {
        return JacksonWriter.INSTANCE;
    }

    /**
     * Get the default ArrayWriter instance that outputs nicely-formatted JSON arrays.
     * @return the shared default pretty writer instance; never null
     */
    static ArrayWriter prettyWriter() {
        return JacksonWriter.PRETTY_WRITER;
    }

    /**
     * Write the supplied array to bytes using UTF-8.
     * @param array the array to be written; may not be null
     * @return the bytes containing the output JSON-formatted array; never null
     */
    default byte[] writeAsBytes(Array array) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            write(array, stream);
            return stream.toByteArray();
        }
        catch (IOException e) {
            // This really should never happen ...
            e.printStackTrace();
            return new byte[]{};
        }
    }

    /**
     * Write the supplied array to bytes using UTF-8.
     * @param array the array to be written; may not be null
     * @param jsonStream the stream to which the array is to be written; may not be null
     * @throws IOException if an array could not be written to the supplied stream
     */
    void write(Array array, OutputStream jsonStream) throws IOException;

    /**
     * Write the supplied array to bytes using UTF-8.
     * @param array the array to be written; may not be null
     * @param jsonWriter the IO writer to which the array is to be written; may not be null
     * @throws IOException if an array could not be written to the supplied stream
     */
    void write(Array array, Writer jsonWriter) throws IOException;

    /**
     * Write the supplied array to a string using UTF-8.
     * @param array the array to be written; may not be null
     * @return the string containing the output JSON-formatted array; never null
     * @throws IOException if an array could not be written to the supplied stream
     */
    String write(Array array) throws IOException;

}
