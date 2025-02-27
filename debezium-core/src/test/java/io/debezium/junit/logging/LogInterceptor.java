/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;

/**
 * @author Chris Cranford, Jiri Pechanec
 */
public class LogInterceptor extends AppenderBase<ILoggingEvent> {
    private List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

    /**
     * Constructor using root logger.
     * This is usually not desirable as disabled additivity can prevent message to get here.
     */
    protected LogInterceptor() {
        try {
            final Logger rootLogger = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
            this.start();
            rootLogger.addAppender(this);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to obtain logback logger for log interceptor.", e);
        }
    }

    /**
     * Provides a log interceptor based on the logger that emits the message.
     *
     * @param loggerName logger that emits the log message
     */
    public LogInterceptor(String loggerName) {
        try {
            final Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
            this.start();
            logger.addAppender(this);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to obtain logback logger for log interceptor.", e);
        }
    }

    /**
     * Provides a log interceptor based on the logger that emits the message.
     *
     * @param clazz class that emits the log message
     */
    public LogInterceptor(Class<?> clazz) {
        this(clazz.getName());
    }

    @Override
    protected void append(ILoggingEvent loggingEvent) {
        this.events.add(loggingEvent);
    }

    public void setLoggerLevel(Class<?> loggerClass, Level level) {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(loggerClass.getName());
        logger.setLevel(level);
    }

    public List<String> getLogEntriesThatContainsMessage(String text) {
        return events.stream()
                .filter(e -> e.getFormattedMessage().toString().contains(text))
                .map(e -> e.getFormattedMessage().toString())
                .collect(Collectors.toList());
    }

    public long countOccurrences(String text) {
        return events.stream().filter(e -> e.getMessage().toString().contains(text)).count();
    }

    public boolean containsMessage(String text) {
        for (ILoggingEvent event : events) {
            if (event.getFormattedMessage().toString().contains(text)) {
                return true;
            }
        }
        return false;
    }

    public boolean messageMatches(String regex) {
        for (ILoggingEvent event : events) {
            if (event.getFormattedMessage().toString().matches(regex)) {
                return true;
            }
        }
        return false;
    }

    public List<ILoggingEvent> getLoggingEvents(String text) {
        List<ILoggingEvent> matchEvents = new ArrayList<>();
        for (ILoggingEvent event : events) {
            if (event.getFormattedMessage().toString().contains(text)) {
                matchEvents.add(event);
            }
        }
        return matchEvents;
    }

    public boolean containsWarnMessage(String text) {
        return containsMessage(Level.WARN, text);
    }

    public boolean containsErrorMessage(String text) {
        return containsMessage(Level.ERROR, text);
    }

    public boolean containsStacktraceElement(String text) {
        for (ILoggingEvent event : events) {
            IThrowableProxy stackTrace = event.getThrowableProxy();
            for (;;) {
                if (stackTrace == null) {
                    break;
                }
                if ((stackTrace.getClassName() + ": " + stackTrace.getMessage()).contains(text)) {
                    return true;
                }
                stackTrace = stackTrace.getCause();
            }
        }
        return false;
    }

    public void clear() {
        events.clear();
    }

    private boolean containsMessage(Level level, String text) {
        for (ILoggingEvent event : events) {
            if (event.getLevel().equals(level) && event.getFormattedMessage().toString().contains(text)) {
                return true;
            }
        }
        return false;
    }
}
