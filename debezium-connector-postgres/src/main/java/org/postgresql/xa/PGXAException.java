/*-------------------------------------------------------------------------
*
* Copyright (c) 2009-2014, PostgreSQL Global Development Group
*
*-------------------------------------------------------------------------
*/

package org.postgresql.xa;

import javax.transaction.xa.XAException;

/**
 * A convenience subclass of <code>XAException</code> which makes it easy to create an instance of
 * <code>XAException</code> with a human-readable message, a <code>Throwable</code> cause, and an XA
 * error code.
 *
 * @author Michael S. Allman
 */
public class PGXAException extends XAException {
  PGXAException(String message, int errorCode) {
    super(message);

    this.errorCode = errorCode;
  }

  PGXAException(String message, Throwable cause, int errorCode) {
    super(message);

    initCause(cause);
    this.errorCode = errorCode;
  }

  PGXAException(Throwable cause, int errorCode) {
    super(errorCode);

    initCause(cause);
  }
}
