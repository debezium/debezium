/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bean;

import io.debezium.DebeziumException;

/**
 * Indicates that a bean lookup failed.
 *
 * @author Chris Cranford
 */
public class NoSuchBeanException extends DebeziumException {

    private final String beanName;

    public NoSuchBeanException(String beanName, String message, Throwable throwable) {
        super(message, throwable);
        this.beanName = beanName;
    }

    /**
     * Get the bean name for which a lookup failed.
     *
     * @return the bean name
     */
    public String getBeanName() {
        return beanName;
    }

}
