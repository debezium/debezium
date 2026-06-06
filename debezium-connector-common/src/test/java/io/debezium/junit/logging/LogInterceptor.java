/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
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
     * Prefer using either {@link #forPackage(String)} or {@link #forName(String)}
     *
     * @param loggerName logger that emits the log message
     */
    public LogInterceptor(String loggerName) {
        try {
            appendInterceptorAsAppender(loggerName);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to obtain logback logger for log interceptor.", e);
        }
    }

    /**
     * Provides a log interceptor based on the logger that emits the message.
     * Prefer using {@link #forClass(Class)}
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

    /**
     * Checks all logged events that had a {@link Throwable} provided that the stack trace
     * contains the given class.
     *
     * @param causeClassName the class to check for
     * @return {@code true} if there was a match; {@code false} otherwise
     */
    public boolean containsThrowableWithCause(Class<?> causeClassName) {
        for (ILoggingEvent event : events) {
            IThrowableProxy throwableProxy = event.getThrowableProxy();
            if (throwableProxy != null) {
                do {
                    if (Objects.equals(throwableProxy.getClassName(), causeClassName.getName())) {
                        return true;
                    }
                    throwableProxy = throwableProxy.getCause();
                } while (throwableProxy != null);
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

    /**
     * Creates a log interceptor for a package to include all classes within that package and any child
     * packages that exist in the class hierarchy.
     *
     * @param packageName the package name
     * @return a new interceptor instance
     */
    public static LogInterceptor forPackage(String packageName) {
        return new LogInterceptor("%s.*".formatted(packageName));
    }

    /**
     * Creates a log interceptor for a specific class.
     *
     * @param clazz the class type
     * @return a new interceptor instance
     */
    public static LogInterceptor forClass(Class<?> clazz) {
        return new LogInterceptor(clazz);
    }

    /**
     * Creates a log interceptor for a specific logger name.
     * <p>
     * Loggers are hierarchical, but do not propagate logged events from child to parent loggers. So when
     * using this specific method, an interceptor will only capture logged events that are explicitly
     * caught by the specified name. If a logback configuration defines a logger that is for a class or
     * package that is a child of the given name, those events will not be propagated and caught by the
     * interceptor. For this use case, use {@link #forPackage}.
     *
     * @param explicitLoggerName the explicit logger name
     * @return a new interceptor instance
     */
    public static LogInterceptor forName(String explicitLoggerName) {
        return new LogInterceptor(explicitLoggerName);
    }

    private void appendInterceptorAsAppender(String name) {
        if (name == null) {
            return;
        }

        this.start();

        String loggerName = name.trim();
        if (loggerName.endsWith(".*")) {
            loggerName = loggerName.substring(0, loggerName.length() - 2);
            // Apply package and child logger appending
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            for (Logger logger : context.getLoggerList()) {
                if (logger.getName().equals(loggerName) || logger.getName().startsWith(loggerName + ".")) {
                    logger.addAppender(this);
                }
            }
        }
        else {
            // Apply to explicit logger only.
            final Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
            logger.addAppender(this);
        }
    }
}
