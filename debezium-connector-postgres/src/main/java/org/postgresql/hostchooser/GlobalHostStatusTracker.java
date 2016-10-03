/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.hostchooser;

import static java.lang.System.currentTimeMillis;

import org.postgresql.util.HostSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of HostSpec targets in a global map.
 */
public class GlobalHostStatusTracker {
  private static final Map<HostSpec, HostSpecStatus> hostStatusMap =
      new HashMap<HostSpec, HostSpecStatus>();

  /**
   * Store the actual observed host status.
   *
   * @param hostSpec The host whose status is known.
   * @param hostStatus Latest known status for the host.
   */
  public static void reportHostStatus(HostSpec hostSpec, HostStatus hostStatus) {
    long now = currentTimeMillis();
    synchronized (hostStatusMap) {
      HostSpecStatus oldStatus = hostStatusMap.get(hostSpec);
      if (oldStatus == null || updateStatusFromTo(oldStatus.status, hostStatus)) {
        hostStatusMap.put(hostSpec, new HostSpecStatus(hostSpec, hostStatus, now));
      }
    }
  }

  private static boolean updateStatusFromTo(HostStatus oldStatus, HostStatus newStatus) {
    if (oldStatus == null) {
      return true;
    }
    if (newStatus == HostStatus.ConnectOK) {
      return oldStatus != HostStatus.Master && oldStatus != HostStatus.Slave;
    }
    return true;
  }

  /**
   * Returns a list of candidate hosts that have the required targetServerType.
   *
   * @param hostSpecs The potential list of hosts.
   * @param targetServerType The required target server type.
   * @param hostRecheckMillis How stale information is allowed.
   * @return candidate hosts to connect to.
   */
  static List<HostSpecStatus> getCandidateHosts(HostSpec[] hostSpecs,
      HostRequirement targetServerType, long hostRecheckMillis) {
    List<HostSpecStatus> candidates = new ArrayList<HostSpecStatus>(hostSpecs.length);
    long latestAllowedUpdate = currentTimeMillis() - hostRecheckMillis;
    synchronized (hostStatusMap) {
      for (HostSpec hostSpec : hostSpecs) {
        HostSpecStatus hostInfo = hostStatusMap.get(hostSpec);
        // return null status wrapper if if the current value is not known or is too old
        if (hostInfo == null || hostInfo.lastUpdated < latestAllowedUpdate) {
          hostInfo = new HostSpecStatus(hostSpec, null, Long.MAX_VALUE);
        }
        // candidates are nodes we do not know about and the nodes with correct type
        if (hostInfo.status == null || targetServerType.allowConnectingTo(hostInfo.status)) {
          candidates.add(hostInfo);
        }
      }
    }
    return candidates;
  }

  /**
   * Immutable structure of known status of one HostSpec.
   */
  static class HostSpecStatus {
    final HostSpec host;
    final HostStatus status;
    final long lastUpdated;

    HostSpecStatus(HostSpec host, HostStatus hostStatus, long lastUpdated) {
      this.host = host;
      this.status = hostStatus;
      this.lastUpdated = lastUpdated;
    }

    @Override
    public String toString() {
      return host.toString() + '=' + status;
    }
  }
}
