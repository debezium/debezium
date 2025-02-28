package io.debezium.sink.spi;

import org.apache.kafka.connect.errors.RetriableException;

public class SinkErrorHandler {

    /**
     * Determines if an exception is retriable.
     *
     * @param throwable The thrown exception.
     * @return True if the exception is retriable, false otherwise.
     */
    public static boolean isRetriable(Throwable throwable) {
        Throwable cause = throwable;

        while (cause != null) {
            if (cause instanceof RetriableException ||
                    cause instanceof ChangeEventSinkRetriableException) {
                return true;
            }
            cause = cause.getCause();
        }

        return false;
    }
}
