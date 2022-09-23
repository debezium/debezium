/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality for dealing with {@link Throwable}s.
 *
 * @author Gunnar Morling
 */
public class Throwables {

    private static final Logger LOGGER = LoggerFactory.getLogger(Throwables.class);

    public static Throwable getRootCause(Throwable throwable) {
        while (true) {
            Throwable cause = throwable.getCause();
            if (cause == null) {
                return throwable;
            }
            throwable = cause;
        }
    }

    public static void logErrorAndTraceRecord(Logger logger, String message, Exception e, Object record) {
        logger.error(message, e);
        LOGGER.trace("Source of error is {}", record);
    }
}
