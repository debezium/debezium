/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import io.debezium.annotation.ThreadSafe;

/**
 * A {@link DocumentWriter} and {@link ArrayWriter} that uses the Jackson library to write JSON.
 *
 * @author Randall Hauch
 */
@ThreadSafe
final class JacksonWriter implements DocumentWriter, ArrayWriter {

    public static final JacksonWriter INSTANCE = new JacksonWriter(false);
    public static final JacksonWriter PRETTY_WRITER = new JacksonWriter(true);

    private static final JsonFactory factory;

    static {
        factory = new JsonFactory();
    }

    private final boolean pretty;

    private JacksonWriter(boolean pretty) {
        this.pretty = pretty;
    }

    @Override
    public void write(Document document, OutputStream jsonStream) throws IOException {
        try (JsonGenerator jsonGenerator = factory.createGenerator(jsonStream)) {
            configure(jsonGenerator);
            writeDocument(document, jsonGenerator);
        }
    }

    @Override
    public void write(Document document, Writer jsonWriter) throws IOException {
        try (JsonGenerator jsonGenerator = factory.createGenerator(jsonWriter)) {
            configure(jsonGenerator);
            writeDocument(document, jsonGenerator);
        }
    }

    @Override
    public String write(Document document) throws IOException {
        StringWriter writer = new StringWriter();
        try (JsonGenerator jsonGenerator = factory.createGenerator(writer)) {
            configure(jsonGenerator);
            writeDocument(document, jsonGenerator);
        }
        return writer.getBuffer().toString();
    }

    @Override
    public byte[] writeAsBytes(Document document) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (JsonGenerator jsonGenerator = factory.createGenerator(stream, JsonEncoding.UTF8)) {
                configure(jsonGenerator);
                writeDocument(document, jsonGenerator);
            }
            return stream.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Array array, OutputStream jsonStream) throws IOException {
        try (JsonGenerator jsonGenerator = factory.createGenerator(jsonStream)) {
            configure(jsonGenerator);
            writeArray(array, jsonGenerator);
        }
    }

    @Override
    public void write(Array array, Writer jsonWriter) throws IOException {
        try (JsonGenerator jsonGenerator = factory.createGenerator(jsonWriter)) {
            configure(jsonGenerator);
            writeArray(array, jsonGenerator);
        }
    }

    @Override
    public String write(Array array) throws IOException {
        StringWriter writer = new StringWriter();
        try (JsonGenerator jsonGenerator = factory.createGenerator(writer)) {
            configure(jsonGenerator);
            writeArray(array, jsonGenerator);
        }
        return writer.getBuffer().toString();
    }

    protected void configure(JsonGenerator generator) {
        if (pretty) {
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
        }
    }

    protected void writeDocument(Document document, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        try {
            document.stream().forEach((field) -> {
                try {
                    generator.writeFieldName(field.getName().toString());
                    writeValue(field.getValue(), generator);
                }
                catch (IOException e) {
                    throw new WritingError(e);
                }
            });
            generator.writeEndObject();
        }
        catch (WritingError e) {
            throw e.wrapped();
        }
    }

    protected void writeArray(Array array, JsonGenerator generator) throws IOException {
        generator.writeStartArray();
        try {
            array.streamValues().forEach((value) -> {
                try {
                    writeValue(value, generator);
                }
                catch (IOException e) {
                    throw new WritingError(e);
                }
            });
            generator.writeEndArray();
        }
        catch (WritingError e) {
            throw e.wrapped();
        }
    }

    protected void writeValue(Value value, JsonGenerator generator) throws IOException {
        switch (value.getType()) {
            case NULL:
                generator.writeNull();
                break;
            case STRING:
                generator.writeString(value.asString());
                break;
            case BOOLEAN:
                generator.writeBoolean(value.asBoolean());
                break;
            case BINARY:
                generator.writeBinary(value.asBytes());
                break;
            case INTEGER:
                generator.writeNumber(value.asInteger());
                break;
            case LONG:
                generator.writeNumber(value.asLong());
                break;
            case FLOAT:
                generator.writeNumber(value.asFloat());
                break;
            case DOUBLE:
                generator.writeNumber(value.asDouble());
                break;
            case BIG_INTEGER:
                generator.writeNumber(value.asBigInteger());
                break;
            case DECIMAL:
                generator.writeNumber(value.asBigDecimal());
                break;
            case DOCUMENT:
                writeDocument(value.asDocument(), generator);
                break;
            case ARRAY:
                writeArray(value.asArray(), generator);
                break;
        }
    }

    protected static final class WritingError extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final IOException wrapped;

        protected WritingError(IOException wrapped) {
            this.wrapped = wrapped;
        }

        public IOException wrapped() {
            return wrapped;
        }
    }

}
