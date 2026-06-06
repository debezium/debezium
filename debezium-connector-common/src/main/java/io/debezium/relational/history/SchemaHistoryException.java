/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * @author Randall Hauch
 *
 */
public class SchemaHistoryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SchemaHistoryException(String message) {
        super(message);
    }

    public SchemaHistoryException(Throwable cause) {
        super(cause);
    }

    public SchemaHistoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaHistoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
