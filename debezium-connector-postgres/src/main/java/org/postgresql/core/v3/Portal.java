/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.core.ResultCursor;
import org.postgresql.core.Utils;

import java.lang.ref.PhantomReference;

/**
 * V3 ResultCursor implementation in terms of backend Portals. This holds the state of a single
 * Portal. We use a PhantomReference managed by our caller to handle resource cleanup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class Portal implements ResultCursor {
  Portal(SimpleQuery query, String portalName) {
    this.query = query;
    this.portalName = portalName;
    this.encodedName = Utils.encodeUTF8(portalName);
  }

  public void close() {
    if (cleanupRef != null) {
      cleanupRef.clear();
      cleanupRef.enqueue();
      cleanupRef = null;
    }
  }

  String getPortalName() {
    return portalName;
  }

  byte[] getEncodedPortalName() {
    return encodedName;
  }

  SimpleQuery getQuery() {
    return query;
  }

  void setCleanupRef(PhantomReference<?> cleanupRef) {
    this.cleanupRef = cleanupRef;
  }

  public String toString() {
    return portalName;
  }

  // Holding on to a reference to the generating query has
  // the nice side-effect that while this Portal is referenced,
  // so is the SimpleQuery, so the underlying statement won't
  // be closed while the portal is open (the backend closes
  // all open portals when the statement is closed)

  private final SimpleQuery query;
  private final String portalName;
  private final byte[] encodedName;
  private PhantomReference<?> cleanupRef;
}
