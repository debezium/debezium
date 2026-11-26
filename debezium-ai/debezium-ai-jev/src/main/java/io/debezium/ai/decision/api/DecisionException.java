/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Failure of an evaluation by a {@link DecisionService}.
 * The message must never contain the state content or credentials.
 *
 * @author Jiri Pechanec
 */
public class DecisionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean retriable;
    private final int httpStatus;

    public DecisionException(final String message, final boolean retriable) {
        this(message, null, retriable, 0);
    }

    public DecisionException(final String message, final Throwable cause, final boolean retriable) {
        this(message, cause, retriable, 0);
    }

    public DecisionException(final String message, final boolean retriable, final int httpStatus) {
        this(message, null, retriable, httpStatus);
    }

    public DecisionException(final String message, final Throwable cause, final boolean retriable, final int httpStatus) {
        super(message, cause);
        this.retriable = retriable;
        this.httpStatus = httpStatus;
    }

    /**
     * @return {@code true} if a repeated attempt may succeed
     */
    public boolean isRetriable() {
        return retriable;
    }

    /**
     * @return HTTP status of the failure or {@code 0} if not applicable
     */
    public int httpStatus() {
        return httpStatus;
    }
}
