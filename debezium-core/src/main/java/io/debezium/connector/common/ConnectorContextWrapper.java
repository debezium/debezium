package io.debezium.connector.common;

public interface ConnectorContextWrapper {

    void requestTaskReconfiguration();

    /**
     * Raise an unrecoverable exception to the Connect framework. This will cause the status of the
     * connector to transition to FAILED.
     * @param e Exception to be raised.
     */
    void raiseError(Exception e);
}
