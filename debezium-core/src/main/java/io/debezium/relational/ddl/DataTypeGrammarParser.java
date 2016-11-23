/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.sql.Types;
import java.util.function.Consumer;
import java.util.function.Function;

import io.debezium.annotation.Immutable;
import io.debezium.text.ParsingException;
import io.debezium.text.TokenStream;
import io.debezium.text.TokenStream.Marker;

/**
 * A parser for data type grammars that produces one {@link DataTypePattern} for a grammar, where the {@link DataTypePattern} can
 * be used when {@link DataTypeParser parsing} content containing data types adhering to the grammar(s).
 * <p>
 * The grammar using the following rules:
 * <ul>
 * <li>Uppercase {@link DdlTokenizer#KEYWORD keyword} or {@link DdlTokenizer#WORD word} tokens are considered literals that much
 * be matched case-insensitively but completely.</li>
 * <li>Lowercase {@link DdlTokenizer#KEYWORD keyword} or {@link DdlTokenizer#WORD word} tokens are considered logical names of
 * a variable and will match and single token.</li>
 * <li>Optional tokens are delimited by '[' and ']' characters.</li>
 * <li>The type length, {@code L} (or mantissa, {@code M}) is specified with '(L)' or '(M)', or if coupled with a scale,
 * {@code S}, with '(L,S)' or '(M,D)'. In these cases, the length will match any positive long, and scale will match any integer
 * value. When the length or scale are represented with integers, they will match only those values.</li>
 * <li>The type may have a length of {@code ...}, in which case the parser will allow (but not capture) a list of values.</li>
 * <li>The literal '<code>{n}</code>' signals that the grammar can allow array dimensions at this point.</li>
 * </ul>
 * <p>
 * For example, the following are valid data type grammars:
 * <ul>
 * <li>{@code BIT}</li>
 * <li>{@code BIT(3)} will match when the length is exactly 3 and will not match {@code BIT(2)}.</li>
 * <li>{@code DECIMAL(L[,S])} will match {@code DECIMAL(5)} and {@code DECIMAL (10,3)}</li>
 * <li><code>INTEGER{n}</code> will match {@code INTEGER[3]}, which is an array of integers.
 * <li>
 * <li><code>ENUM(...)</code> will match {@code ENUM(a,b,c,d)} and {@code ENUM(a)}.
 * <li>
 * </ul>
 * 
 * @author Randall Hauch
 * @see DataTypeParser
 */
@Immutable
public class DataTypeGrammarParser {

    public static class DataTypePattern {
        private final Pattern pattern;
        private final DataTypeBuilder builder = new DataTypeBuilder();
        private final int jdbcType;

        protected DataTypePattern(Pattern pattern, int jdbcType) {
            this.pattern = pattern;
            this.jdbcType = jdbcType;
        }

        /**
         * Get the JDBC type associated with this data type.
         * 
         * @return the JDBC {@link Types JDBC type} constant
         */
        public int jdbcType() {
            return jdbcType;
        }

        /**
         * Look for a matching data type on the specified token stream.
         * 
         * @param stream the stream of tokens containing the data type definition
         * @return the data type, or null if no data type could be found
         */
        public DataType match(TokenStream stream) {
            return match(stream, null);
        }

        /**
         * Look for a matching data type on the specified token stream.
         * 
         * @param stream the stream of tokens containing the data type definition
         * @param errors the function called for each parsing exception; may be null if not needed
         * @return the data type, or null if no data type could be found
         */
        public DataType match(TokenStream stream, Consumer<ParsingException> errors) {
            builder.reset();
            builder.jdbcType = jdbcType;
            if (pattern.match(stream, builder, errors != null ? errors : (e) -> {})) {
                return builder.create();
            }
            return null;
        }

        public void forEachFirstToken(Consumer<String> tokens) {
            pattern.determineFirstTokens(tokens);
        }

        @Override
        public String toString() {
            return pattern.toString();
        }
    }

    private final DdlTokenizer tokenizer = new DdlTokenizer(true);

    /**
     * Create a new instance of the data type grammar parser.
     */
    public DataTypeGrammarParser() {
    }

    /**
     * Parse the supplied grammar for a data type.
     * 
     * @param jdbcType the {@link Types JDBC data type}
     * @param dataTypeDefn the data type grammar
     * @return the data type pattern that can be used for pattern matching the data type; never null
     * @throws ParsingException if the grammar cannot be parsed correctly
     */
    public DataTypePattern parse(int jdbcType, String dataTypeDefn) throws ParsingException {
        TokenStream stream = new TokenStream(dataTypeDefn, tokenizer, false);
        stream.start();
        Pattern pattern = parseMultiple(stream);
        return pattern != null ? new DataTypePattern(pattern, jdbcType) : null;
    }

    protected Pattern parseMultiple(TokenStream stream) throws ParsingException {
        Pattern pattern = null;
        while (stream.hasNext()) {
            Pattern inner = parsePattern(stream);
            if (inner == null) return pattern;
            if ( stream.canConsume('|')) {
                Pattern orPattern = parseMultiple(stream);
                inner = new OrPattern(inner,orPattern);
            }
            pattern = pattern == null ? inner : new AndPattern(pattern, inner);
        }
        return pattern;
    }

    protected Pattern parsePattern(TokenStream stream) throws ParsingException {
        if (stream.matches('[')) {
            return parseOptional(stream, this::parseMultiple);
        }
        if (stream.matches('(')) {
            // This is a length ...
            return parseLength(stream);
        }
        if (stream.matches('{')) {
            return parseArrayDimensions(stream);
        }
        if (stream.matchesAnyOf(DdlTokenizer.KEYWORD, DdlTokenizer.WORD)) {
            // This is a literal ...
            String literal = stream.consume();
            if (literal.toLowerCase().equals(literal)) {
                // This contains all lowercase, so treat it as a variable name ...
                return new VariablePattern(literal);
            }
            return new LiteralPattern(literal);
        }
        return null;
    }

    protected Pattern parseOptional(TokenStream stream, Function<TokenStream, Pattern> inside) throws ParsingException {
        stream.consume('[');
        Pattern pattern = inside.apply(stream);
        stream.consume(']');
        return new OptionalPattern(pattern);
    }

    protected Pattern parseArrayDimensions(TokenStream stream) throws ParsingException {
        stream.consume('{').consume('n').consume('}');
        return new ArrayDimensionsPattern();
    }

    protected Pattern parseLength(TokenStream stream) throws ParsingException {
        stream.consume('(');
        Pattern result = new LiteralPattern("(", false);

        if (stream.canConsume(".", ".", ".")) {
            // This is a list pattern ...
            result = new AndPattern(result, new ListPattern());
        } else if (stream.canConsumeAnyOf("L", "M", "P", "N")) {
            // specifies length, mantissa, precision, or number ...
            result = new AndPattern(result, new LengthPattern());
        } else {
            // This should be at least one literal ...
            Pattern literal = parseLengthLiteral(stream);
            while (stream.canConsume('|')) {
                // This is an OR-ed list of literals ...
                literal = new OrPattern(literal, parseLengthLiteral(stream));
            }
            result = new AndPattern(result, literal);
        }

        Pattern scale = null;
        if (stream.matches(',')) {
            scale = parseScale(stream);
        } else if (stream.matches('[')) {
            scale = parseOptional(stream, this::parseScale);
        }
        if (scale != null) {
            result = new AndPattern(result, scale);
        }
        stream.consume(')');
        return new AndPattern(result, new LiteralPattern(")", false));
    }

    protected Pattern parseScale(TokenStream stream) throws ParsingException {
        stream.consume(',');
        Pattern result = new LiteralPattern(",", false);

        if (stream.canConsume('S') || stream.canConsume('D')) { // "scale" or "decimal"
            // This is a length pattern ...
            result = new AndPattern(result, new ScalePattern());
        } else {
            // This should be at least one literal ...
            Pattern literal = parseScaleLiteral(stream);
            while (stream.canConsume('|')) {
                // This is an OR-ed list of literals ...
                literal = new OrPattern(literal, parseScaleLiteral(stream));
            }
            result = new AndPattern(result, literal);
        }
        return result;
    }

    protected Pattern parseLengthLiteral(TokenStream stream) throws ParsingException {
        long lengthLiteral = stream.consumeLong();
        return new LiteralLengthPattern(lengthLiteral);

    }

    protected Pattern parseScaleLiteral(TokenStream stream) throws ParsingException {
        int scaleLiteral = stream.consumeInteger();
        return new LiteralScalePattern(scaleLiteral);

    }

    protected static class DataTypeBuilder {
        private StringBuilder prefix = new StringBuilder();
        private StringBuilder suffix = new StringBuilder();
        private String parameters;
        private int jdbcType = Types.NULL;
        private long length = -1;
        private int scale = -1;
        private int arrayDimsLength = 0;
        private final int[] arrayDims = new int[40];

        public void addToName(String str) {
            if (length == -1) {
                // Length hasn't been set yet, so add to the prefix ...
                if (prefix.length() != 0) prefix.append(' ');
                prefix.append(str);
            } else {
                // Length has already been set, so add as a suffix ...
                if (suffix.length() != 0) suffix.append(' ');
                suffix.append(str);
            }
        }

        public DataTypeBuilder parameters(String parameters) {
            this.parameters = parameters;
            return this;
        }

        public DataTypeBuilder length(long length) {
            this.length = length;
            return this;
        }

        public DataTypeBuilder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public DataTypeBuilder addArrayDimension(int dimension) {
            arrayDims[arrayDimsLength++] = dimension;
            return this;
        }

        public DataTypeBuilder reset() {
            length = -1;
            scale = -1;
            arrayDimsLength = 0;
            prefix.setLength(0);
            suffix.setLength(0);
            return this;
        }

        public DataType create() {
            StringBuilder name = new StringBuilder(this.prefix);
            StringBuilder expression = new StringBuilder(this.prefix);
            if (length != -1) {
                expression.append('(');
                expression.append(this.length);
                if (scale != -1) {
                    expression.append(',');
                    expression.append(this.scale);
                }
                expression.append(')');
            } else if (parameters != null ) {
                expression.append('(');
                expression.append(parameters);
                expression.append(')');
            }
            if (arrayDimsLength != 0) {
                for (int i = 0; i != arrayDimsLength; ++i) {
                    expression.append('[');
                    expression.append(this.arrayDims[i]);
                    expression.append(']');
                }
            }
            if (suffix.length() != 0) {
                expression.append(' ');
                expression.append(suffix);
                name.append(' ');
                name.append(suffix);
            }
            return new DataType(expression.toString(), name.toString(), jdbcType, length, scale, arrayDims, arrayDimsLength);
        }
    }

    protected static interface Pattern {
        boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error);

        default boolean isOptional() {
            return false;
        }

        default boolean determineFirstTokens(Consumer<String> tokens) {
            return false;
        }
    }

    protected static class AndPattern implements Pattern {
        private final Pattern pattern1;
        private final Pattern pattern2;

        public AndPattern(Pattern pattern1, Pattern pattern2) {
            this.pattern1 = pattern1;
            this.pattern2 = pattern2;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            Marker marker = stream.mark();
            try {
                if (pattern1.match(stream, builder, error) && pattern2.match(stream, builder, error)) return true;
            } catch (ParsingException e) {
                stream.rewind(marker);
            }
            return false;
        }

        @Override
        public boolean determineFirstTokens(Consumer<String> tokens) {
            if (!pattern1.determineFirstTokens(tokens)) return false;
            return pattern2.determineFirstTokens(tokens);
        }

        @Override
        public String toString() {
            return pattern1.toString() + " " + pattern2.toString();
        }
    }

    protected static class OrPattern implements Pattern {
        private final Pattern pattern1;
        private final Pattern pattern2;

        public OrPattern(Pattern pattern1, Pattern pattern2) {
            this.pattern1 = pattern1;
            this.pattern2 = pattern2;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            Marker marker = stream.mark();
            try {
                if (pattern1.match(stream, builder, error)) return true;
            } catch (ParsingException e) {}
            stream.rewind(marker);
            try {
                if (pattern2.match(stream, builder, error)) return true;
            } catch (ParsingException e) {}
            stream.rewind(marker);
            return false;
        }

        @Override
        public boolean determineFirstTokens(Consumer<String> tokens) {
            return false;
        }

        @Override
        public String toString() {
            return pattern1.toString() + " | " + pattern2.toString();
        }
    }

    protected static class LiteralPattern implements Pattern {
        protected final String literal;
        protected final boolean addToBuilder;

        public LiteralPattern(String literal) {
            this(literal, true);
        }

        public LiteralPattern(String literal, boolean addToBuilder) {
            this.literal = literal;
            this.addToBuilder = addToBuilder;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            stream.consume(literal);
            if (addToBuilder) builder.addToName(literal);
            return true;
        }

        @Override
        public boolean determineFirstTokens(Consumer<String> tokens) {
            tokens.accept(literal);
            return false;
        }

        @Override
        public String toString() {
            return literal;
        }
    }

    protected static class VariablePattern extends LiteralPattern {

        public VariablePattern(String variableName) {
            this(variableName, true);
        }

        public VariablePattern(String variableName, boolean addToBuilder) {
            super(variableName, addToBuilder);
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            String variableName = stream.consume();
            if (addToBuilder) builder.addToName(variableName);
            return true;
        }

        @Override
        public boolean determineFirstTokens(Consumer<String> tokens) {
            return false;
        }

        @Override
        public String toString() {
            return literal;
        }
    }

    protected static class ListPattern implements Pattern {

        private final String delimiter;

        public ListPattern() {
            this.delimiter = ",";
        }

        public ListPattern(String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            if (stream.matches(')')) {
                // empty list ...
                return true;
            }
            Marker start = stream.mark();
            stream.consume(); // first item
            while (stream.canConsume(delimiter)) {
                stream.consume();
            }
            // Read the parameters ...
            String parameters = stream.getContentFrom(start);
            builder.parameters(parameters);
            return true;
        }

        @Override
        public boolean determineFirstTokens(Consumer<String> tokens) {
            return false;
        }

        @Override
        public String toString() {
            return "LIST";
        }
    }

    protected static class OptionalPattern implements Pattern {
        private final Pattern pattern;

        public OptionalPattern(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            if (stream.hasNext()) {
                Marker marker = stream.mark();
                try {
                    if (!pattern.match(stream, builder, error)) {
                        stream.rewind(marker);
                    }
                } catch (ParsingException e) {
                    error.accept(e);
                    stream.rewind(marker);
                }
            }
            return true;
        }

        @Override
        public boolean isOptional() {
            return true;
        }

        @Override
        public boolean determineFirstTokens(Consumer<String> tokens) {
            return pattern.determineFirstTokens(tokens);
        }

        @Override
        public String toString() {
            return "[" + pattern.toString() + "]";
        }
    }

    protected static class LengthPattern implements Pattern {
        public LengthPattern() {
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            Marker marker = stream.mark();
            long value = stream.consumeLong();
            if (value >= 0) {
                // Must be non-negative ...
                builder.length(value);
                return true;
            }
            stream.rewind(marker);
            return false;
        }

        @Override
        public String toString() {
            return "L";
        }
    }

    protected static class ScalePattern implements Pattern {

        public ScalePattern() {
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            builder.scale(stream.consumeInteger()); // can be positive or negative
            return true;
        }

        @Override
        public String toString() {
            return "S";
        }
    }

    protected static class LiteralLengthPattern implements Pattern {
        private final long literal;

        public LiteralLengthPattern(long literal) {
            this.literal = literal;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            if (stream.consumeLong() == literal) {
                builder.length(literal);
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return Long.toString(literal);
        }
    }

    protected static class LiteralScalePattern implements Pattern {
        private final int literal;

        public LiteralScalePattern(int literal) {
            this.literal = literal;
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            if (stream.consumeInteger() == literal) {
                builder.scale(literal);
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return Integer.toString(literal);
        }
    }

    protected static class ArrayDimensionsPattern implements Pattern {

        public ArrayDimensionsPattern() {
        }

        @Override
        public boolean match(TokenStream stream, DataTypeBuilder builder, Consumer<ParsingException> error) {
            while (stream.canConsume('[')) {
                int dimension = stream.consumeInteger();
                stream.consume(']');
                builder.addArrayDimension(dimension);
            }
            return true;
        }

        @Override
        public String toString() {
            return "arrayDims";
        }
    }
}
