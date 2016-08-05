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

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.FederationApplicationHomeSubClusterStore;
import org.apache.hadoop.yarn.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.util.MonotonicClock;

import com.google.common.annotations.VisibleForTesting;

/**
 * In-memory implementation of FederationMembershipStateStore and
 * FederationApplicationHomeSubClusterStore.
 */
public class MemoryFederationStateStore implements
    FederationMembershipStateStore, FederationApplicationHomeSubClusterStore {

  private final Map<SubClusterId, SubClusterInfo> membership =
      new ConcurrentHashMap<SubClusterId, SubClusterInfo>();

  private final Map<ApplicationId, SubClusterId> applications =
      new ConcurrentHashMap<ApplicationId, SubClusterId>();

  private final MonotonicClock clock = new MonotonicClock();

  @Override
  public Version getMembershipStateStoreVersion() {
    return null;
  }

  ///// FederationMembershipStateStore APIs

  @Override
  public SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest request) throws YarnException {
    SubClusterInfo subClusterInfo = request.getSubClusterInfo();
    subClusterInfo.setLastStartTime(clock.getTime());
    membership.put(subClusterInfo.getSubClusterId(), subClusterInfo);
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  public SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest request) throws YarnException {
    SubClusterInfo subClusterInfo = membership.get(request.getSubClusterId());
    if (subClusterInfo == null) {
      throw new YarnException(
          "SubCluster " + request.getSubClusterId().toString() + " not found");
    } else {
      subClusterInfo.setState(request.getState());
    }

    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  public SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest request) throws YarnException {

    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterInfo subClusterInfo = membership.get(subClusterId);

    if (subClusterInfo == null) {
      throw new YarnException("Subcluster " + subClusterId.toString()
          + " does not exist; cannot heartbeat");
    }

    subClusterInfo.setLastHeartBeat(clock.getTime());
    subClusterInfo.setState(request.getState());
    subClusterInfo.setCapability(request.getCapability());

    return SubClusterHeartbeatResponse.newInstance();
  }

  @Override
  public GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest request) throws YarnException {
    SubClusterId subClusterId = request.getSubClusterId();
    if (!membership.containsKey(subClusterId)) {
      throw new YarnException(
          "Subcluster " + subClusterId.toString() + " does not exist");
    }

    return GetSubClusterInfoResponse.newInstance(membership.get(subClusterId));
  }

  @Override
  public GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest request) throws YarnException {
    List<SubClusterInfo> result = new ArrayList<SubClusterInfo>();

    for (SubClusterInfo info : membership.values()) {
      if (!request.getFilterInactiveSubClusters()
          || info.getState().isActive()) {
        result.add(info);
      }
    }
    return GetSubClustersInfoResponse.newInstance(result);

  }

  ///// FederationApplicationHomeSubClusterStore APIs

  @Override
  public Version getApplicationStateStoreVersion() {
    return null;
  }

  @Override
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubClusterMap(
      AddApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();
    if (applications.containsKey(appId)) {
      throw new YarnException(
          "Application " + appId.toString() + " already exists");
    }

    applications.put(appId,
        request.getApplicationHomeSubCluster().getHomeSubCluster());
    return AddApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubClusterMap(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException("Application " + appId.toString() + " not found");
    }

    applications.put(appId,
        request.getApplicationHomeSubCluster().getHomeSubCluster());
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubClusterMap(
      GetApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationId();

    if (appId == null) {
      throw new YarnException("appId is null");
    }
    if (!applications.containsKey(appId)) {
      throw new YarnException("Application " + appId.toString() + " not found");
    }

    ApplicationHomeSubCluster appHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, applications.get(appId));
    return GetApplicationHomeSubClusterResponse.newInstance(appHomeSubCluster);
  }

  @Override
  public GetApplicationsHomeSubClusterResponse getApplicationsHomeSubClusterMap(
      GetApplicationsHomeSubClusterRequest request) throws YarnException {
    List<ApplicationHomeSubCluster> result =
        new ArrayList<ApplicationHomeSubCluster>();

    for (Entry<ApplicationId, SubClusterId> e : applications.entrySet()) {
      result
          .add(ApplicationHomeSubCluster.newInstance(e.getKey(), e.getValue()));
    }

    return GetApplicationsHomeSubClusterResponse.newInstance(result);
  }

  @Override
  public DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubClusterMap(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException("Application " + appId.toString() + " not found");
    }

    applications.remove(appId);
    return DeleteApplicationHomeSubClusterResponse.newInstance();
  }

  ///// Test convenience methods

  @VisibleForTesting
  public Map<SubClusterId, SubClusterInfo> getMembershipTable() {
    return membership;
  }

  @VisibleForTesting
  public void clearMembershipTable() {
    membership.clear();
  }

  @VisibleForTesting
  public Map<ApplicationId, SubClusterId> getApplicationsTable() {
    return applications;
  }

  @VisibleForTesting
  public void clearApplicationsTable() {
    applications.clear();
  }
}
