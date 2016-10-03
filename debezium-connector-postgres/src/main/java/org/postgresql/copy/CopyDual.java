package org.postgresql.copy;

/**
 * Bidirectional via copy stream protocol. Via bidirectional copy protocol work PostgreSQL
 * replication.
 *
 * @see CopyIn
 * @see CopyOut
 */
public interface CopyDual extends CopyIn, CopyOut {
}
