/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.ddl.DataTypeGrammarParser.DataTypePattern;
import io.debezium.text.ParsingException;
import io.debezium.text.TokenStream;
import io.debezium.text.TokenStream.Marker;

/**
 * A parser of SQL data types. Callers set up a parser and register patterns that describe the possible lists of data type tokens,
 * and then repeatedly {@link #parse(TokenStream, Consumer) parse} {@link TokenStream streams of tokens} looking for matches.
 * <p>
 * This is typically used within a {@link DdlParser} implementation to parse and identify data types appearing within a stream
 * of DDL content.
 * 
 * @author Randall Hauch
 * @see DdlParser
 */
@ThreadSafe
public class DataTypeParser {

    private final Map<String, Collection<DataTypePattern>> patterns = new ConcurrentHashMap<>();
    private final DataTypeGrammarParser parser = new DataTypeGrammarParser();

    /**
     * Create an empty data type parser with no data types registered.
     */
    public DataTypeParser() {
    }

    /**
     * Register data type that may not contain a length/precision or scale.
     * <p>
     * The order that grammars are registered is the same order that they are evaluated by the resulting parser. However, when
     * multiple grammars match the same input tokens, the grammar that successfully consumes the most input tokens will be
     * selected.
     * 
     * @param jdbcType the best JDBC type for the data type
     * @param grammar the grammar the defines the data type format; may not be null
     * @return this parser so callers can chain methods; never null
     */
    public DataTypeParser register(int jdbcType, String grammar) {
        Objects.requireNonNull(grammar, "the data type grammar must be specified");
        DataTypePattern pattern = parser.parse(jdbcType, grammar);
        pattern.forEachFirstToken(token -> {
            patterns.computeIfAbsent(token, (key) -> new ArrayList<DataTypePattern>()).add(pattern);
        });
        return this;
    }

    /**
     * Examine the stream starting at its current position for a matching data type. If this method finds a matching data type,
     * it will consume the stream of all tokens that make up the data type. However, if no data type is found, the stream is left
     * unchanged.
     * <p>
     * This method looks for data types that match those registered patterns.
     * This method also looks for multi-dimensional arrays, where any registered data type pattern is followed by one or more
     * array dimensions of the form {@code [n]}, where {@code n} is the integer dimension.
     * <p>
     * Sometimes, a data type matches one of the registered patterns but it contains a malformed length/precision, scale, and/or
     * array dimensions. These parsing exceptions can be reported back to the caller via the supplied {@code errorHandler},
     * although these errors are only reported when no data type is found. This is often useful when the caller expects
     * to find a data type, but no such data type can be found due to one or more parsing exceptions. When this happens,
     * the method calls {@code errorHandler} with all of the {@link ParsingException}s and then returns <code>null</code>.
     * 
     * @param stream the stream of tokens; may not be null
     * @param errorHandler a function that should be called when no data type was found because at least one
     *            {@link ParsingException}
     *            occurred with the length/precision, scale, and/or array dimensions; may be null
     * @return the data type if one was found, or null if none were found
     */
    public DataType parse(TokenStream stream, Consumer<Collection<ParsingException>> errorHandler) {
        if (stream.hasNext()) {
            // Look for all patterns that begin with the first token ...
            Collection<DataTypePattern> matchingPatterns = patterns.get(stream.peek().toUpperCase());
            if (matchingPatterns != null) {
                // At least one registered type begins with the first token, so go through them all in order ...
                ErrorCollector errors = new ErrorCollector();
                Marker mostReadMarker = null;
                DataType mostReadType = null;
                Marker marker = stream.mark();
                for (DataTypePattern pattern : matchingPatterns) {
                    DataType result = pattern.match(stream, errors::record);
                    if (result != null) {
                        // We found a match, so record it if it is better than our previous best ...
                        if (!stream.hasNext()) {
                            // There's no more to read, so we should be done ...
                            return result;
                        }
                        Marker endMarker = stream.mark();
                        if (mostReadMarker == null || endMarker.compareTo(mostReadMarker) > 0) {
                            mostReadMarker = endMarker;
                            mostReadType = result;
                        }
                    }
                    stream.rewind(marker); // always, even in the case of success
                }
                if (mostReadType != null) {
                    // We've found at least one, so advance the stream to the end of what was consumed by that data type
                    // and return the type that consumes the most of the stream ...
                    stream.advance(mostReadMarker);
                    return mostReadType;
                }
                // We still haven't found a data type ...
                errors.send(errorHandler);
            }
        }
        // Ultimately did not find a match ...
        return null;
    }

    protected static final class ErrorCollector {
        protected Collection<ParsingException> errors = null;

        protected void record(ParsingException e) {
            if (errors == null) errors = new ArrayList<>();
            errors.add(e);
        }

        protected void send(Consumer<Collection<ParsingException>> errorHandler) {
            if (errorHandler != null && errors != null) {
                errorHandler.accept(errors);
            }
        }
    }
}
