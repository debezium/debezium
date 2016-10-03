/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.hostchooser;

/**
 * Describes the required server type.
 */
public enum HostRequirement {
  any {
    public boolean allowConnectingTo(HostStatus status) {
      return status != HostStatus.ConnectFail;
    }
  },
  master {
    public boolean allowConnectingTo(HostStatus status) {
      return status == HostStatus.Master || status == HostStatus.ConnectOK;
    }
  },
  slave {
    public boolean allowConnectingTo(HostStatus status) {
      return status == HostStatus.Slave || status == HostStatus.ConnectOK;
    }
  },
  preferSlave {
    public boolean allowConnectingTo(HostStatus status) {
      return status != HostStatus.ConnectFail;
    }
  };

  public abstract boolean allowConnectingTo(HostStatus status);
}
