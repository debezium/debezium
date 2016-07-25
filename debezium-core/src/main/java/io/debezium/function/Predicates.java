/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.debezium.util.Strings;

/**
 * Utilities for constructing various predicates.
 * @author Randall Hauch
 *
 */
public class Predicates {

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>any</i> of the regular expressions in
     * the supplied comma-separated list matches the predicate parameter.
     * 
     * @param regexPatterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static Predicate<String> includes(String regexPatterns) {
        return includes(regexPatterns, (str) -> str);
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>none</i> of the regular
     * expressions in the supplied comma-separated list matches the predicate parameter.
     * 
     * @param regexPatterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static Predicate<String> excludes(String regexPatterns) {
        return includes(regexPatterns).negate();
    }

    /**
     * Generate a predicate function that for any supplied parameter returns {@code true} if <i>any</i> of the regular expressions
     * in the supplied comma-separated list matches the predicate parameter in a case-insensitive manner.
     * 
     * @param regexPatterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @param conversion the function that converts each predicate-supplied value to a string that can be matched against the
     *            regular expressions; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static <T> Predicate<T> includes(String regexPatterns, Function<T, String> conversion) {
        Set<Pattern> patterns = Strings.listOfRegex(regexPatterns,Pattern.CASE_INSENSITIVE);
        return (t) -> {
            String str = conversion.apply(t);
            if ( str != null ) {
                for ( Pattern p : patterns ) {
                    if ( p.matcher(str).matches()) return true;
                }
            }
            return false;
        };
    }

    /**
     * Generate a predicate function that for any supplied parameter returns {@code true} if <i>none</i> of the regular
     * expressions in the supplied comma-separated list matches the predicate parameter.
     * 
     * @param regexPatterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @param conversion the function that converts each predicate-supplied value to a string that can be matched against the
     *            regular expressions; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static <T> Predicate<T> excludes(String regexPatterns, Function<T, String> conversion) {
        return includes(regexPatterns, conversion).negate();
    }

    /**
     * Create a predicate function that allows only those values are allowed or not disallowed by the supplied predicates.
     * 
     * @param allowed the predicate that defines the allowed values; may be null
     * @param disallowed the predicate that defines the disallowed values; may be null
     * @return the predicate function; never null
     */
    public static <T> Predicate<T> filter( Predicate<T> allowed, Predicate<T> disallowed ) {
        return allowed != null ? allowed : (disallowed != null ? disallowed : (id)->true);
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }

    public static <T> Predicate<T> notNull() {
        return new Predicate<T>() {
            @Override
            public boolean test(T t) {
                return t != null;
            }
        };
    }

    private Predicates() {
    }

}
