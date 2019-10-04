/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import io.debezium.annotation.ThreadSafe;

/**
 * A {@link DocumentReader} and {@link ArrayReader} that uses the Jackson library to read JSON.
 *
 * @author Randall Hauch
 */
@ThreadSafe
final class JacksonReader implements DocumentReader, ArrayReader {

    public static final JacksonReader DEFAULT_INSTANCE = new JacksonReader(false);
    public static final JacksonReader FLOAT_NUMBERS_AS_TEXT_INSTANCE = new JacksonReader(true);

    private static final JsonFactory factory;
    private final boolean handleFloatNumbersAsText;

    static {
        factory = new JsonFactory();
        factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
        factory.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
    }

    private JacksonReader(boolean handleFloatNumbersAsText) {
        this.handleFloatNumbersAsText = handleFloatNumbersAsText;
    }

    @Override
    public Document read(InputStream jsonStream) throws IOException {
        return parse(factory.createParser(jsonStream));
    }

    @Override
    public Document read(Reader jsonReader) throws IOException {
        return parse(factory.createParser(jsonReader));
    }

    @Override
    public Document read(String json) throws IOException {
        return parse(factory.createParser(json));
    }

    @Override
    public Document read(File jsonFile) throws IOException {
        return parse(factory.createParser(jsonFile));
    }

    @Override
    public Document read(URL jsonUrl) throws IOException {
        return parse(factory.createParser(jsonUrl));
    }

    @Override
    public Document read(byte[] rawBytes) throws IOException {
        return parse(factory.createParser(rawBytes));
    }

    @Override
    public Array readArray(InputStream jsonStream) throws IOException {
        return parseArray(factory.createParser(jsonStream), false);
    }

    @Override
    public Array readArray(Reader jsonReader) throws IOException {
        return parseArray(factory.createParser(jsonReader), false);
    }

    @Override
    public Array readArray(URL jsonUrl) throws IOException {
        return parseArray(factory.createParser(jsonUrl), false);
    }

    @Override
    public Array readArray(File jsonFile) throws IOException {
        return parseArray(factory.createParser(jsonFile), false);
    }

    @Override
    public Array readArray(String jsonArray) throws IOException {
        return parseArray(factory.createParser(jsonArray), false);
    }

    private Document parse(JsonParser parser) throws IOException {
        try {
            return parseDocument(parser, false);
        }
        finally {
            parser.close();
        }
    }

    private Document parseDocument(JsonParser parser, boolean nested) throws IOException {
        // Iterate over the fields in the top-level document ...
        BasicDocument doc = new BasicDocument();
        JsonToken token = null;
        if (!nested) {
            // We expect the START_OBJECT token ...
            token = parser.nextToken();
            if (!nested && token != JsonToken.START_OBJECT) {
                throw new IOException("Expected data to start with an Object, but was " + token);
            }
        }
        String fieldName = null;
        token = parser.nextToken();
        while (token != JsonToken.END_OBJECT) {
            switch (token) {
                case FIELD_NAME:
                    fieldName = parser.getCurrentName();
                    break;
                case START_OBJECT:
                    doc.setDocument(fieldName, parseDocument(parser, true));
                    break;
                case START_ARRAY:
                    doc.setArray(fieldName, parseArray(parser, true));
                    break;
                case VALUE_STRING:
                    doc.setString(fieldName, parser.getValueAsString());
                    break;
                case VALUE_TRUE:
                    doc.setBoolean(fieldName, true);
                    break;
                case VALUE_FALSE:
                    doc.setBoolean(fieldName, false);
                    break;
                case VALUE_NULL:
                    doc.setNull(fieldName);
                    break;
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                    switch (parser.getNumberType()) {
                        case FLOAT:
                            if (handleFloatNumbersAsText) {
                                doc.setString(fieldName, parser.getText());
                            }
                            else {
                                doc.setNumber(fieldName, parser.getFloatValue());
                            }
                            break;
                        case DOUBLE:
                            if (handleFloatNumbersAsText) {
                                doc.setString(fieldName, parser.getText());
                            }
                            else {
                                doc.setNumber(fieldName, parser.getDoubleValue());
                            }
                            break;
                        case BIG_DECIMAL:
                            if (handleFloatNumbersAsText) {
                                doc.setString(fieldName, parser.getText());
                            }
                            else {
                                doc.setNumber(fieldName, parser.getDecimalValue());
                            }
                            break;
                        case INT:
                            doc.setNumber(fieldName, parser.getIntValue());
                            break;
                        case LONG:
                            doc.setNumber(fieldName, parser.getLongValue());
                            break;
                        case BIG_INTEGER:
                            doc.setNumber(fieldName, parser.getBigIntegerValue());
                            break;
                    }
                    break;
                case VALUE_EMBEDDED_OBJECT:
                    // disregard this, since it's an extension ...
                    break;
                case NOT_AVAILABLE:
                    throw new JsonParseException(parser, "Non-blocking parsers are not supported", parser.getCurrentLocation());
                case END_ARRAY:
                    throw new JsonParseException(parser, "Not expecting an END_ARRAY token", parser.getCurrentLocation());
                case END_OBJECT:
                    throw new JsonParseException(parser, "Not expecting an END_OBJECT token", parser.getCurrentLocation());
            }
            token = parser.nextToken();
        }
        return doc;
    }

    private Array parseArray(JsonParser parser, boolean nested) throws IOException {
        // Iterate over the values in the array ...
        BasicArray array = new BasicArray();
        JsonToken token = null;
        if (!nested) {
            // We expect the START_ARRAY token ...
            token = parser.nextToken();
            if (!nested && token != JsonToken.START_ARRAY) {
                throw new IOException("Expected data to start with an Array, but was " + token);
            }
        }
        token = parser.nextToken();
        while (token != JsonToken.END_ARRAY) {
            switch (token) {
                case START_OBJECT:
                    array.add(parseDocument(parser, true));
                    break;
                case START_ARRAY:
                    array.add(parseArray(parser, true));
                    break;
                case VALUE_STRING:
                    array.add(parser.getValueAsString());
                    break;
                case VALUE_TRUE:
                    array.add(true);
                    break;
                case VALUE_FALSE:
                    array.add(false);
                    break;
                case VALUE_NULL:
                    array.addNull();
                    break;
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                    switch (parser.getNumberType()) {
                        case FLOAT:
                            if (handleFloatNumbersAsText) {
                                array.add(parser.getText());
                            }
                            else {
                                array.add(parser.getFloatValue());
                            }
                            break;
                        case DOUBLE:
                            if (handleFloatNumbersAsText) {
                                array.add(parser.getText());
                            }
                            else {
                                array.add(parser.getDoubleValue());
                            }
                            break;
                        case BIG_DECIMAL:
                            if (handleFloatNumbersAsText) {
                                array.add(parser.getText());
                            }
                            else {
                                array.add(parser.getDecimalValue());
                            }
                            break;
                        case INT:
                            array.add(parser.getIntValue());
                            break;
                        case LONG:
                            array.add(parser.getLongValue());
                            break;
                        case BIG_INTEGER:
                            array.add(parser.getBigIntegerValue());
                            break;
                    }
                    break;
                case VALUE_EMBEDDED_OBJECT:
                    // disregard this, since it's an extension ...
                    break;
                case NOT_AVAILABLE:
                    throw new JsonParseException(parser, "Non-blocking parsers are not supported", parser.getCurrentLocation());
                case FIELD_NAME:
                    throw new JsonParseException(parser, "Not expecting a FIELD_NAME token", parser.getCurrentLocation());
                case END_ARRAY:
                    throw new JsonParseException(parser, "Not expecting an END_ARRAY token", parser.getCurrentLocation());
                case END_OBJECT:
                    throw new JsonParseException(parser, "Not expecting an END_OBJECT token", parser.getCurrentLocation());
            }
            token = parser.nextToken();
        }
        return array;
    }
}
