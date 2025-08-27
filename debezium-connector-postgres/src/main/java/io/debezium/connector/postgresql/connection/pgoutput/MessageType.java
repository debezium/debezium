package io.debezium.connector.postgresql.connection.pgoutput;

public enum MessageType {
    RELATION,
    BEGIN,
    COMMIT,
    INSERT,
    UPDATE,
    DELETE,
    TYPE,
    ORIGIN,
    TRUNCATE,
    LOGICAL_DECODING_MESSAGE,
    STREAM_START,
    STREAM_STOP,
    STREAM_COMMIT,
    STREAM_ABORT;

    public static MessageType forType(char type) {
        switch (type) {
            case 'R':
                return RELATION;
            case 'B':
                return BEGIN;
            case 'C':
                return COMMIT;
            case 'I':
                return INSERT;
            case 'U':
                return UPDATE;
            case 'D':
                return DELETE;
            case 'Y':
                return TYPE;
            case 'O':
                return ORIGIN;
            case 'T':
                return TRUNCATE;
            case 'M':
                return LOGICAL_DECODING_MESSAGE;
            case 'S':
                return STREAM_START;
            case 'E':
                return STREAM_STOP;
            case 'c':
                return STREAM_COMMIT;
            case 'A':
                return STREAM_ABORT;
            default:
                throw new IllegalArgumentException("Unsupported message type: " + type);
        }
    }
}