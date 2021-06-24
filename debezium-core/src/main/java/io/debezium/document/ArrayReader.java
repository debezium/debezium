/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import io.debezium.annotation.ThreadSafe;

/**
 * Reads {@link Array} instances from a variety of input forms.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public interface ArrayReader {

    /**
     * Get the default {@link ArrayReader} instance.
     *
     * @return the shared default reader instance; never null
     */
    static ArrayReader defaultReader() {
        return JacksonReader.DEFAULT_INSTANCE;
    }

    /**
     * Read an array from the supplied stream.
     *
     * @param jsonStream the input stream to be read; may not be null
     * @return the array instance; never null
     * @throws IOException if an array could not be read from the supplied stream
     */
    Array readArray(InputStream jsonStream) throws IOException;

    /**
     * Read an array from the supplied {@link Reader}.
     *
     * @param jsonReader the reader to be read; may not be null
     * @return the array instance; never null
     * @throws IOException if an array could not be read from the supplied reader
     */
    Array readArray(Reader jsonReader) throws IOException;

    /**
     * Read an array from the supplied JSON-formatted string.
     *
     * @param json the JSON string representation to be read; may not be null
     * @return the array instance; never null
     * @throws IOException if an array could not be read from the supplied string
     */
    Array readArray(String json) throws IOException;

    /**
     * Read an array from the content at the given URL.
     *
     * @param jsonUrl the URL to the content that is to be read; may not be null
     * @return the array instance; never null
     * @throws IOException if an array could not be read from the supplied content
     */
    default Array readArray(URL jsonUrl) throws IOException {
        return readArray(jsonUrl.openStream());
    }

    /**
     * Read an array from the supplied file.
     *
     * @param jsonFile the file to be read; may not be null
     * @return the array instance; never null
     * @throws IOException if an array could not be read from the supplied file
     */
    default Array readArray(File jsonFile) throws IOException {
        return readArray(new BufferedInputStream(new FileInputStream(jsonFile)));
    }

    /**
     * Read an array from the supplied bytes.
     *
     * @param rawBytes the UTF-8 bytes to be read; may not be null
     * @return the array instance; never null
     * @throws IOException if an array could not be read from the supplied bytes
     */
    default Array readArray(byte[] rawBytes) throws IOException {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(rawBytes)) {
            return ArrayReader.defaultReader().readArray(stream);
        }
    }

}
