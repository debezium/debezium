/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.function.Predicate;

public class ExceptionUtils {

    private ExceptionUtils() {
    }

    public static boolean testInExceptionHierarchy(Throwable throwable, Predicate<Throwable> predicate) {
        Throwable cause = throwable;
        while (cause != null) {
            if (predicate.test(cause)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

}
