/*
 * Copyright 2015 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.util.function.Predicate;

/**
 * @author Randall Hauch
 *
 */
public class Predicates {

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
