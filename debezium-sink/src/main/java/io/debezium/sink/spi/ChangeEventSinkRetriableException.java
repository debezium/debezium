package io.debezium.sink.spi;

public class ChangeEventSinkRetriableException extends RuntimeException {
    public ChangeEventSinkRetriableException(String message) {
        super(message);
    }

    public ChangeEventSinkRetriableException(String message, Throwable cause) {
        super(message, cause);
    }
}
