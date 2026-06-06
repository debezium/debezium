/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality for dealing with logging.
 *
 * @author Chris Cranford
 */
public class Loggings {

    private static final Logger LOGGER = LoggerFactory.getLogger(Loggings.class);

    /**
     * Log a warning message and explicitly append the source of the warning as a separate log entry that uses
     * trace logging to prevent unintended leaking of sensitive data.
     *
     * @param logger the logger instance
     * @param record the record the log entry is based upon
     * @param message the warning message to be logged
     * @param arguments the arguments passed to the warning message
     */
    public static void logWarningAndTraceRecord(Logger logger, Object record, String message, Object... arguments) {
        logger.warn(message, arguments);
        LOGGER.trace("Source of warning is record '{}'", record);
    }

    /**
     * Log a debug message and explicitly append the source of the debug entry as a separate log entry that uses
     * trace logging to prevent unintended leaking of sensitive data.
     *
     * @param logger the logger instance
     * @param record the record the log entry is based upon
     * @param message the debug message to be logged
     * @param arguments the arguments passed to the debug message
     */
    public static void logDebugAndTraceRecord(Logger logger, Object record, String message, Object... arguments) {
        logger.debug(message, arguments);
        LOGGER.trace("Source of debug is record '{}'", record);
    }

    /**
     * Log an error message and explicitly append the source of the error entry as a separate log entry that uses
     * trace logging to prevent unintended leaking of sensitive data.
     *
     * @param logger the logger instance
     * @param record the record the log entry is based upon
     * @param message the error message to be logged
     * @param arguments the arguments passed to the error message
     */
    public static void logErrorAndTraceRecord(Logger logger, Object record, String message, Object... arguments) {
        logger.error(message, arguments);
        LOGGER.trace("Source of error is record '{}'", record);
    }

    /**
     * Log an error message and explicitly append the source of the error entry as a separate log entry that uses
     * trace logging to prevent unintended leaking of sensitive data.
     *
     * @param logger the logger instance
     * @param record the record the log entry is based upon
     * @param message the error message to be logged
     * @param t the exception that caused the error
     */
    public static void logErrorAndTraceRecord(Logger logger, Object record, String message, Throwable t) {
        logger.error(message, t);
        LOGGER.trace("Source of error is record '{}'", record);
    }

    /**
     * Redact sensitive data in the log entry if sensitive logging is not enabled.
     * @param value the value to be redacted
     * @return the redacted value if sensitive logging is not enabled, otherwise the original value
     */
    public static Object maybeRedactSensitiveData(Object value) {
        if (LOGGER.isTraceEnabled()) {
            return value;
        }
        return "[REDACTED]";
    }

    /**
     * Redact sensitive data in the log entry if sensitive logging is not enabled.
     * @param value the value to be redacted
     * @return the redacted value if sensitive logging is not enabled, otherwise the original value
     */
    public static Object maybeRedactSensitiveData(Object[] value) {
        if (LOGGER.isTraceEnabled()) {
            return Arrays.toString(value);
        }
        return "[REDACTED]";
    }
}
