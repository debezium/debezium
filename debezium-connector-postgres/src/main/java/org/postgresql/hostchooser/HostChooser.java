/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.hostchooser;

import org.postgresql.util.HostSpec;

import java.util.Iterator;

/**
 * Lists connections in preferred order.
 */
public interface HostChooser {
  /**
   * Lists connection hosts in preferred order.
   *
   * @return connection hosts in preferred order.
   */
  Iterator<HostSpec> iterator();
}
