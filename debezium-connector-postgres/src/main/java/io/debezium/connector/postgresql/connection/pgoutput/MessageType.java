/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection.pgoutput;

/**
 * Represents the different types of replication messages encountered in PostgreSQL logical decoding.
 * <p>
 * Each enum constant corresponds to a specific message type identified by a single character code
 * from the replication stream.
 * </p>
 * <p>
 * This enum facilitates mapping from the raw message type character to a meaningful enum constant.
 * </p>
 *
 * <ul>
 *   <li>RELATION — Relation metadata message</li>
 *   <li>BEGIN — Transaction begin</li>
 *   <li>COMMIT — Transaction commit</li>
 *   <li>INSERT — Insert operation</li>
 *   <li>UPDATE — Update operation</li>
 *   <li>DELETE — Delete operation</li>
 *   <li>TYPE — Type metadata message</li>
 *   <li>ORIGIN — Replication origin message</li>
 *   <li>TRUNCATE — Truncate operation</li>
 *   <li>LOGICAL_DECODING_MESSAGE — Logical decoding message</li>
 *   <li>STREAM_START — Streaming replication start</li>
 *   <li>STREAM_STOP — Streaming replication stop</li>
 *   <li>STREAM_COMMIT — Streaming replication commit</li>
 *   <li>STREAM_ABORT — Streaming replication abort</li>
 * </ul>
 *
 * @author Pranav Tiwari
 */
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

    /**
     * Returns the {@link MessageType} corresponding to the given replication message type character.
     *
     * @param type the single character code representing the message type
     * @return the corresponding {@link MessageType} enum constant
     * @throws IllegalArgumentException if the character does not map to any known message type
     */
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
