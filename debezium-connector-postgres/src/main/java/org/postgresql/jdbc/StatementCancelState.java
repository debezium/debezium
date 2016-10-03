/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

/**
 * Represents {@link PgStatement#cancel()} state.
 */
enum StatementCancelState {
    IDLE,
    IN_QUERY,
    CANCELING,
    CANCELLED
}
