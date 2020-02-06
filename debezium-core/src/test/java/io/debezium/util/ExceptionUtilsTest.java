/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import org.fest.assertions.Assertions;
import org.junit.Test;

public class ExceptionUtilsTest {

    @Test
    public void findsAtTheBeginningOfHierarchy() {
        Throwable throwable = new Exception("1", new Exception("2", new Exception("3")));

        boolean actual = ExceptionUtils.testInExceptionHierarchy(throwable, throwable1 -> "1".equals(throwable1.getMessage()));

        Assertions.assertThat(actual).isTrue();
    }

    @Test
    public void findsInTheMiddleOfHierarchy() {
        Throwable throwable = new Exception("1", new Exception("2", new Exception("3")));

        boolean actual = ExceptionUtils.testInExceptionHierarchy(throwable, throwable1 -> "2".equals(throwable1.getMessage()));

        Assertions.assertThat(actual).isTrue();
    }

    @Test
    public void findsAtTheEndOfHierarchy() {
        Throwable throwable = new Exception("1", new Exception("2", new Exception("3")));

        boolean actual = ExceptionUtils.testInExceptionHierarchy(throwable, throwable1 -> "3".equals(throwable1.getMessage()));

        Assertions.assertThat(actual).isTrue();
    }

    @Test
    public void returnsFalseWhenCantBeFound() {
        Throwable throwable = new Exception("1", new Exception("2", new Exception("3")));

        boolean actual = ExceptionUtils.testInExceptionHierarchy(throwable, throwable1 -> "4".equals(throwable1.getMessage()));

        Assertions.assertThat(actual).isFalse();
    }
}