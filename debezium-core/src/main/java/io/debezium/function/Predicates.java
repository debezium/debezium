/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.debezium.util.Strings;

/**
 * Utilities for constructing various predicates.
 *
 * @author Randall Hauch
 *
 */
public class Predicates {

    private static final Pattern LITERAL_SEPARATOR_PATTERN = Pattern.compile(",");

    /**
     * Generate a predicate function that for any supplied UUID strings returns {@code true} if <i>any</i> of the comma-separated
     * UUID literals or regular expressions matches the predicate parameter. This supplied strings can be a mixture
     * of regular expressions and UUID literals, and the most efficient method will be used for each.
     *
     * @param uuidPatterns the comma-separated UUID literals or regular expression patterns; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static Predicate<String> includesUuids(String uuidPatterns) {
        return includesLiteralsOrPatterns(uuidPatterns, Strings::isUuid, (s) -> s);
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>none</i> of the regular
     * expressions or literals in the supplied comma-separated list matches the predicate parameter. This supplied strings can be
     * a mixture of regular expressions and UUID literals, and the most efficient method will be used for each.
     *
     * @param uuidPatterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static Predicate<String> excludesUuids(String uuidPatterns) {
        return includesUuids(uuidPatterns).negate();
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>any</i> of the regular expressions
     * or literals in the supplied comma-separated list matches the predicate parameter. This supplied strings can be a mixture
     * of regular expressions and literals, and the most efficient method will be used for each.
     *
     * @param literalsOrPatterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @param isLiteral function that determines if a given pattern is a literal string; may not be null
     * @param conversion the function that converts each predicate-supplied value to a string that can be matched against the
     *            regular expressions; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static <T> Predicate<T> includesLiteralsOrPatterns(String literalsOrPatterns, Predicate<String> isLiteral,
                                                              Function<T, String> conversion) {
        // First create the predicates that handle either literals or patterns ...
        Set<String> literals = new HashSet<>();
        List<Pattern> patterns = new ArrayList<>();
        for (String literalOrPattern : LITERAL_SEPARATOR_PATTERN.split(literalsOrPatterns)) {
            if (isLiteral.test(literalOrPattern)) {
                literals.add(literalOrPattern.toLowerCase());
            }
            else {
                patterns.add(Pattern.compile(literalOrPattern, Pattern.CASE_INSENSITIVE));
            }
        }
        Predicate<T> patternsPredicate = includedInPatterns(patterns, conversion);
        Predicate<T> literalsPredicate = includedInLiterals(literals, conversion);

        // Now figure out which predicate(s) we need to use ...
        if (patterns.isEmpty()) {
            return literalsPredicate;
        }
        if (literals.isEmpty()) {
            return patternsPredicate;
        }
        return literalsPredicate.or(patternsPredicate);
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>none</i> of the regular
     * expressions or literals in the supplied comma-separated list matches the predicate parameter. This supplied strings can be
     * a mixture of regular expressions and literals, and the most efficient method will be used for each.
     *
     * @param patterns the comma-separated regular expression pattern (or literal) strings; may not be null
     * @param isLiteral function that determines if a given pattern is a literal string; may not be null
     * @param conversion the function that converts each predicate-supplied value to a string that can be matched against the
     *            regular expressions; may not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static <T> Predicate<T> excludesLiteralsOrPatterns(String patterns, Predicate<String> isLiteral,
                                                              Function<T, String> conversion) {
        return includesLiteralsOrPatterns(patterns, isLiteral, conversion).negate();
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>any</i> of the literals in
     * the supplied comma-separated list case insensitively matches the predicate parameter.
     *
     * @param literals the comma-separated literal strings; may not be null
     * @return the predicate function that performs the matching
     */
    public static Predicate<String> includesLiterals(String literals) {
        return includesLiterals(literals, (s) -> s);
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>none</i> of the literals in
     * the supplied comma-separated list case insensitively matches the predicate parameter.
     *
     * @param literals the comma-separated literal strings; may not be null
     * @return the predicate function that performs the matching
     */
    public static Predicate<String> excludesLiterals(String literals) {
        return includesLiterals(literals).negate();
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>any</i> of the literals in
     * the supplied comma-separated list case insensitively matches the predicate parameter.
     *
     * @param literals the comma-separated literal strings; may not be null
     * @param conversion the function that converts each predicate-supplied value to a string that can be matched against the
     *            regular expressions; may not be null
     * @return the predicate function that performs the matching
     */
    public static <T> Predicate<T> includesLiterals(String literals, Function<T, String> conversion) {
        String[] literalValues = LITERAL_SEPARATOR_PATTERN.split(literals.toLowerCase());
        Set<String> literalSet = new HashSet<>(Arrays.asList(literalValues));
        return includedInLiterals(literalSet, conversion);
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>none</i> of the literals in
     * the supplied comma-separated list case insensitively matches the predicate parameter.
     *
     * @param literals the comma-separated literal strings; may not be null
     * @param conversion the function that converts each predicate-supplied value to a string that can be matched against the
     *            regular expressions; may not be null
     * @return the predicate function that performs the matching
     */
    public static <T> Predicate<T> excludesLiterals(String literals, Function<T, String> conversion) {
        return includesLiterals(literals, conversion).negate();
    }

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

    public static Predicate<String> includes(String regexPatterns, int regexFlags) {
        Set<Pattern> patterns = Strings.setOfRegex(regexPatterns, regexFlags);
        return includedInPatterns(patterns, str -> str);
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

    public static Predicate<String> excludes(String regexPatterns, int regexFlags) {
        return includes(regexPatterns, regexFlags).negate();
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
        Set<Pattern> patterns = Strings.setOfRegex(regexPatterns, Pattern.CASE_INSENSITIVE);
        return includedInPatterns(patterns, conversion);
    }

    public static <T, U> BiPredicate<T, U> includes(String regexPatterns, BiFunction<T, U, String> conversion) {
        Set<Pattern> patterns = Strings.setOfRegex(regexPatterns, Pattern.CASE_INSENSITIVE);
        return includedInPatterns(patterns, conversion);
    }

    protected static <T> Predicate<T> includedInPatterns(Collection<Pattern> patterns, Function<T, String> conversion) {
        return (t) -> matchedByPattern(patterns, conversion).apply(t).isPresent();
    }

    protected static <T, U> BiPredicate<T, U> includedInPatterns(Collection<Pattern> patterns, BiFunction<T, U, String> conversion) {
        return (t, u) -> matchedByPattern(patterns, conversion).apply(t, u).isPresent();
    }

    /**
     * Generate a predicate function that for any supplied string returns a {@link Pattern} representing the first regular expression
     * in the supplied comma-separated list that matches the predicate parameter in a case-insensitive manner.
     *
     * @param regexPatterns the comma-separated regular expression pattern (or literal) strings; may not be null

     * @return the function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static Function<String, Optional<Pattern>> matchedBy(String regexPatterns) {
        return matchedByPattern(Strings.setOfRegex(regexPatterns, Pattern.CASE_INSENSITIVE), Function.identity());
    }

    protected static <T> Function<T, Optional<Pattern>> matchedByPattern(Collection<Pattern> patterns, Function<T, String> conversion) {
        return (t) -> {
            String str = conversion.apply(t);
            if (str != null) {
                for (Pattern p : patterns) {
                    if (p.matcher(str).matches()) {
                        return Optional.of(p);
                    }
                }
            }
            return Optional.empty();
        };
    }

    protected static <T, U> BiFunction<T, U, Optional<Pattern>> matchedByPattern(Collection<Pattern> patterns, BiFunction<T, U, String> conversion) {
        return (t, u) -> {
            String str = conversion.apply(t, u);
            if (str != null) {
                for (Pattern p : patterns) {
                    if (p.matcher(str).matches()) {
                        return Optional.of(p);
                    }
                }
            }
            return Optional.empty();
        };
    }

    protected static <T> Predicate<T> includedInLiterals(Collection<String> literals, Function<T, String> conversion) {
        return (s) -> {
            String str = conversion.apply(s).toLowerCase();
            return literals.contains(str);
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
    public static <T> Predicate<T> filter(Predicate<T> allowed, Predicate<T> disallowed) {
        return allowed != null ? allowed : (disallowed != null ? disallowed : (id) -> true);
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
