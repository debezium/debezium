/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;

/**
 * @author Chris Cranford
 */
class AssertingAppender extends AbstractAppender {

    private final List<LogEvent> events = new CopyOnWriteArrayList<>();
    private final Logger rootLogger;

    private AssertingAppender(Logger rootLogger) {
        super("LogInterceptor", null, null, false, null);
        this.rootLogger = rootLogger;
    }

    public static AssertingAppender register() {
        Logger rootLogger = (Logger) LogManager.getRootLogger();

        AssertingAppender interceptor = new AssertingAppender(rootLogger);
        interceptor.start();

        rootLogger.addAppender(interceptor);

        return interceptor;
    }

    public void unregister() {
        rootLogger.removeAppender(this);
    }

    public boolean containsMessage(String text) {
        for (LogEvent event : events) {
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
        for (LogEvent event : events) {
            Throwable thrown = event.getThrown();

            if (thrown == null) {
                continue;
            }

            if (containsMessage(thrown, text)) {
                return true;
            }

            StackTraceElement[] stackTrace = thrown.getStackTrace();
            if (stackTrace == null) {
                continue;
            }

            for (StackTraceElement element : stackTrace) {
                if (element.toString().contains(text)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean containsMessage(Throwable throwable, String text) {
        while (throwable != null) {
            if (throwable.getMessage() != null && throwable.getMessage().contains(text)) {
                return true;
            }

            throwable = throwable.getCause();
        }

        return false;
    }

    private boolean containsMessage(Level level, String text) {
        for (LogEvent event : events) {
            if (event.getLevel().equals(level) && event.getMessage().toString().contains(text)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void append(LogEvent event) {
        events.add(event);
    }
}
