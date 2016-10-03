/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.hostchooser;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.Collections.sort;

import org.postgresql.hostchooser.GlobalHostStatusTracker.HostSpecStatus;
import org.postgresql.util.HostSpec;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * HostChooser that keeps track of known host statuses.
 */
public class MultiHostChooser implements HostChooser {
  private HostSpec[] hostSpecs;
  private final HostRequirement targetServerType;
  private int hostRecheckTime;
  private boolean loadBalance;

  protected MultiHostChooser(HostSpec[] hostSpecs, HostRequirement targetServerType,
      Properties info) {
    this.hostSpecs = hostSpecs;
    this.targetServerType = targetServerType;
    hostRecheckTime = parseInt(info.getProperty("hostRecheckSeconds", "10")) * 1000;
    loadBalance = parseBoolean(info.getProperty("loadBalanceHosts", "false"));
  }

  public Iterator<HostSpec> iterator() {
    List<HostSpecStatus> candidates =
        GlobalHostStatusTracker.getCandidateHosts(hostSpecs, targetServerType, hostRecheckTime);
    // if no candidates are suitable (all wrong type or unavailable) then we try original list in
    // order
    if (candidates.isEmpty()) {
      return asList(hostSpecs).iterator();
    }
    if (candidates.size() == 1) {
      return asList(candidates.get(0).host).iterator();
    }
    sortCandidates(candidates);
    shuffleGoodHosts(candidates);
    return extractHostSpecs(candidates).iterator();
  }

  private void sortCandidates(List<HostSpecStatus> candidates) {
    if (targetServerType == HostRequirement.any) {
      return;
    }
    sort(candidates, new HostSpecByTargetServerTypeComparator());
  }

  private void shuffleGoodHosts(List<HostSpecStatus> candidates) {
    if (!loadBalance) {
      return;
    }
    int count;
    for (count = 1; count < candidates.size(); count++) {
      HostSpecStatus hostSpecStatus = candidates.get(count);
      if (hostSpecStatus.status != null
          && !targetServerType.allowConnectingTo(hostSpecStatus.status)) {
        break;
      }
    }
    if (count == 1) {
      return;
    }
    List<HostSpecStatus> goodHosts = candidates.subList(0, count);
    shuffle(goodHosts);
  }

  private List<HostSpec> extractHostSpecs(List<HostSpecStatus> hostSpecStatuses) {
    List<HostSpec> hostSpecs = new ArrayList<HostSpec>(hostSpecStatuses.size());
    for (HostSpecStatus hostSpecStatus : hostSpecStatuses) {
      hostSpecs.add(hostSpecStatus.host);
    }
    return hostSpecs;
  }

  class HostSpecByTargetServerTypeComparator implements Comparator<HostSpecStatus> {
    @Override
    public int compare(HostSpecStatus o1, HostSpecStatus o2) {
      int r1 = rank(o1.status, targetServerType);
      int r2 = rank(o2.status, targetServerType);
      return r1 == r2 ? 0 : r1 > r2 ? -1 : 1;
    }

    private int rank(HostStatus status, HostRequirement targetServerType) {
      if (status == HostStatus.ConnectFail) {
        return -1;
      }
      switch (targetServerType) {
        case master:
          return status == HostStatus.Master || status == null ? 1 : 0;
        case slave:
          return status == HostStatus.Slave || status == null ? 1 : 0;
        case preferSlave:
          return status == HostStatus.Slave || status == null ? 2
              : status == HostStatus.Master ? 1 : 0;
        default:
          return 0;
      }
    }
  }
}
