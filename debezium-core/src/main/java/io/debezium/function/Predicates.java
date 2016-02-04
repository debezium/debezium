/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Randall Hauch
 *
 */
public class Predicates {


    /**
     * Generate a whitelist filter/predicate that allows only those values that <em>are</em> included in the supplied input.
     * 
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static <T> Predicate<T> whitelist(String input, Function<String, String[]> splitter, Function<String, T> factory) {
        if ( input == null ) return (str)->false;
        Set<T> matches = new HashSet<>();
        for (String item : splitter.apply(input)) {
            T obj = factory.apply(item);
            if ( obj != null ) matches.add(obj);
        }
        return matches::contains;
    }

    /**
     * Generate a whitelist filter/predicate that allows only those values that <em>are</em> included in the supplied input.
     * 
     * @param input the input string
     * @param delimiter the character used to delimit the items in the input
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static <T> Predicate<T> whitelist(String input, char delimiter, Function<String, T> factory) {
        return whitelist(input, (str) -> str.split("[" + delimiter + "]"), factory);
    }

    /**
     * Generate a whitelist filter/predicate that allows only those values that <em>are</em> included in the supplied
     * comma-separated input.
     * 
     * @param input the input string
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static <T> Predicate<T> whitelist(String input, Function<String, T> factory) {
        return whitelist(input, (str) -> str.split("[\\,]"), factory);
    }

    /**
     * Generate a whitelist filter/predicate that allows only those values that <em>are</em> included in the supplied
     * comma-separated input.
     * 
     * @param input the input string
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static Predicate<String> whitelist(String input) {
        return whitelist(input, (str) -> str);
    }

    /**
     * Generate a blacklist filter/predicate that allows only those values that are <em>not</em> included in the supplied input.
     * 
     * @param input the input string
     * @param splitter the function that splits the input into multiple items; may not be null
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static <T> Predicate<T> blacklist(String input, Function<String, String[]> splitter, Function<String, T> factory) {
        return whitelist(input, splitter, factory).negate();
    }

    /**
     * Generate a blacklist filter/predicate that allows only those values that are <em>not</em> included in the supplied input.
     * 
     * @param input the input string
     * @param delimiter the character used to delimit the items in the input
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static <T> Predicate<T> blacklist(String input, char delimiter, Function<String, T> factory) {
        return whitelist(input, delimiter, factory).negate();
    }

    /**
     * Generate a blacklist filter/predicate that allows only those values that are <em>not</em> included in the supplied comma-separated input.
     * 
     * @param input the input string
     * @param factory the factory for creating string items into filter matches; may not be null
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static <T> Predicate<T> blacklist(String input, Function<String, T> factory) {
        return whitelist(input, factory).negate();
    }
    /**
     * Generate a blacklist filter/predicate that allows only those values that are <em>not</em> included in the supplied comma-separated input.
     * 
     * @param input the input string
     * @return the predicate that returns {@code true} if and only if the argument to the predicate matches (with
     *         {@link Object#equals(Object) equals(...)} one of the objects parsed from the input; never null
     */
    public static Predicate<String> blacklist(String input) {
        return whitelist(input).negate();
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
