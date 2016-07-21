/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.api.FederationMembershipState;
import org.apache.hadoop.yarn.server.federation.api.records.FederationMembershipStateVersion;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterState;
import org.apache.hadoop.yarn.util.MonotonicClock;

import com.google.common.annotations.VisibleForTesting;

public class FederationInMemoryMembershipState
    implements FederationMembershipState {

  private final Map<FederationSubClusterId, FederationSubClusterInfo> membership =
      new ConcurrentHashMap<FederationSubClusterId, FederationSubClusterInfo>();
  private final MonotonicClock clock = new MonotonicClock();

  @Override
  public FederationMembershipStateVersion getMembershipStateVersion() {
    return null;
  }

  @Override
  public FederationSubClusterId registerSubCluster(
      FederationSubClusterInfo subClusterInfo) throws YarnException {
    subClusterInfo.setLastStartTime(clock.getTime());

    membership.put(subClusterInfo.getSubClusterId(), subClusterInfo);
    return subClusterInfo.getSubClusterId();
  }

  @Override
  public FederationSubClusterDeregisterResponse deregisterSubCluster(
      FederationSubClusterDeregisterRequest request) throws YarnException {

    FederationSubClusterInfo subClusterInfo =
        membership.get(request.getSubClusterId());

    if (subClusterInfo == null) {
      throw new YarnException(
          "SubCluster " + request.getSubClusterId().toString() + " not found");
    } else {
      subClusterInfo.setState(FederationSubClusterState.SC_DEREGISTERED);
    }

    return FederationSubClusterDeregisterResponse.newInstance();
  }

  @Override
  public FederationSubClusterHeartbeatResponse subClusterHeartbeat(
      FederationSubClusterInfo subClusterInfo) throws YarnException {
    FederationSubClusterId subClusterId = subClusterInfo.getSubClusterId();
    FederationSubClusterInfo oldInfo = membership.get(subClusterId);

    if (oldInfo == null) {
      throw new YarnException("Subcluster " + subClusterId.toString()
          + " does not exist; cannot heartbeat");
    }

    subClusterInfo.setLastHeartBeat(clock.getTime());
    membership.put(subClusterId, subClusterInfo);

    return FederationSubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public FederationSubClusterInfo getSubClusterInfo(
      FederationSubClusterId subClusterId) throws YarnException {

    return membership.get(subClusterId);

  }

  @Override
  public Map<FederationSubClusterId, FederationSubClusterInfo> getAllSubClustersInfo()
      throws YarnException {

    Map<FederationSubClusterId, FederationSubClusterInfo> result =
        new HashMap<FederationSubClusterId, FederationSubClusterInfo>();

    for (FederationSubClusterInfo info : membership.values()) {
      if (info.getState().equals(FederationSubClusterState.SC_RUNNING)) {
        result.put(info.getSubClusterId(), info);
      }
    }

    return result;
  }

  @VisibleForTesting
  public Map<FederationSubClusterId, FederationSubClusterInfo> getMembershipTable() {
    return membership;
  }

  @VisibleForTesting
  public void clearMembershipTable() {
    membership.clear();
  }
}
