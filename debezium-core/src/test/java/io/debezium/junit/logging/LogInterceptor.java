/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

/**
 * @author Chris Cranford
 */
public class LogInterceptor extends AppenderSkeleton {
    private List<LoggingEvent> events = new CopyOnWriteArrayList<>();

    public LogInterceptor() {
        try {
            final Field field = Log4jLoggerAdapter.class.getDeclaredField("logger");
            field.setAccessible(true);

            Logger logger = (Logger) field.get(LoggerFactory.getLogger(ROOT_LOGGER_NAME));
            logger.addAppender(this);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to obtain Log4j logger for log interceptor.");
        }
    }

    public LogInterceptor(Class<?> clazz) {
        try {
            final Field field = Log4jLoggerAdapter.class.getDeclaredField("logger");
            field.setAccessible(true);

            Logger logger = (Logger) field.get(LoggerFactory.getLogger(clazz));
            logger.addAppender(this);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to obtain Log4j logger for log interceptor.");
        }
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        this.events.add(loggingEvent);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public boolean containsMessage(String text) {
        for (LoggingEvent event : events) {
            if (event.getMessage().toString().contains(text)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsWarnMessage(String text) {
        return containsMessage(Level.WARN, text);
    }

    public boolean containsErrorMessage(String text) {
        return containsMessage(Level.ERROR, text);
    }

    public boolean containsStacktraceElement(String text) {
        for (LoggingEvent event : events) {
            final String[] stackTrace = event.getThrowableStrRep();
            if (stackTrace == null) {
                continue;
            }
            for (String element : stackTrace) {
                if (element.contains(text)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containsMessage(Level level, String text) {
        for (LoggingEvent event : events) {
            if (event.getLevel().equals(level) && event.getMessage().toString().contains(text)) {
                return true;
            }
        }
        return false;
    }
}
