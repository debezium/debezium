/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Extension of {@link PooledPortResolver} able to parse ports from either {@link String} value or system property
 */
public class ParsingPortResolver extends PooledPortResolver {

    /**
     * Creates port resolver form given property
     *
     * @param property property name
     * @throws NullPointerException when property is undefined or {@code null}
     * @return port resolver
     */
    public static ParsingPortResolver parseProperty(String property) {
        return parseProperty(property, null);
    }

    /**
     * Creates port resolver form given property
     *
     * @param property property name
     * @throws NullPointerException when property is undefined or {@code null}
     * @return port resolver
     */
    public static ParsingPortResolver parseProperty(String property, String defaultValue) {
        var value = System.getProperty(property, defaultValue);
        Objects.requireNonNull(value, "Property '" + property + "' is either undefined or null");
        return new ParsingPortResolver(System.getProperty(property, defaultValue));
    }

    /**
     * Creates port resolver, using build-in strategies to parse the value
     *
     * @param value given value
     */
    public ParsingPortResolver(String value) {
        this(value, new ListParseStrategy(), new RangeParseStrategy());
    }

    /**
     * Creates port resolver, using first usable strategy to parse the value
     *
     * @param value given value
     * @param strategies parsing strategies
     */
    public ParsingPortResolver(String value, ParseStrategy... strategies) {
        this(value, matchingStrategy(value, strategies));
    }

    /**
     * Creates port resolver, using given strategy to parse the value
     *
     * @param value given value
     * @param strategy parsing strategy
     */
    public ParsingPortResolver(String value, ParseStrategy strategy) {
        super(strategy.parse(value));
    }

    private static ParseStrategy matchingStrategy(String value, ParseStrategy... strategies) {
        return Arrays.stream(strategies)
                .filter(s -> s.matches(value))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No applicable parsing strategy"));
    }

    /**
     * Base class for property parsing strategy
     */
    public static abstract class ParseStrategy {
        protected final Pattern pattern;

        /**
         * @param pattern regex pattern describing parseable values
         */
        public ParseStrategy(Pattern pattern) {
            this.pattern = pattern;
        }

        /**
         * Checks if value can be parsed
         *
         * @param value given value
         * @return true if value can be parsed, false otherwise
         */
        public boolean matches(String value) {
            return pattern.asMatchPredicate().test(value);
        }

        /**
         * Parses ports from value
         *
         * @param value given value
         * @throws IllegalArgumentException if given value cannot be parsed
         * @return set of ports
         */
        public Set<Integer> parse(String value) {
            var matcher = pattern.matcher(value);

            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid value '" + value + "'");
            }
            return doParse(matcher, value);
        }

        protected abstract Set<Integer> doParse(Matcher matcher, String value);
    }

    /**
     * Parses ports from comma separated list of numbers. E.g. {@code 8080,8081,8083}
     */
    public static final class ListParseStrategy extends ParseStrategy {
        public static final Pattern PATTERN = Pattern.compile("^\\d+(,\\d+)*$");

        public ListParseStrategy() {
            super(PATTERN);
        }

        @Override
        protected Set<Integer> doParse(Matcher matcher, String value) {
            return Arrays.stream(value.split(","))
                    .map(Integer::parseInt)
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Parses ports from a closed range of number. E.g. {@code 8080:8083}
     */
    public static final class RangeParseStrategy extends ParseStrategy {
        public static final Pattern PATTERN = Pattern.compile("^(?<from>\\d+):(?<to>\\d+)$");

        public RangeParseStrategy() {
            super(PATTERN);
        }

        @Override
        protected Set<Integer> doParse(Matcher matcher, String value) {
            var from = Integer.parseInt(matcher.group("from"));
            var to = Integer.parseInt(matcher.group("to"));

            return IntStream.rangeClosed(from, to)
                    .boxed()
                    .collect(Collectors.toSet());
        }

    }
}
