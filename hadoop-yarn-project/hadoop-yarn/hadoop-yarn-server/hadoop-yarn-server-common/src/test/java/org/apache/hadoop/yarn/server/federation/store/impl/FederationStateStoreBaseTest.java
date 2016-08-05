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

import java.io.IOException;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationApplicationHomeSubClusterStore;
import org.apache.hadoop.yarn.server.federation.store.FederationMembershipStateStore;
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
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for FederationMembershipStateStore implementations.
 */
public abstract class FederationStateStoreBaseTest {

  static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreBaseTest.class);
  private static final MonotonicClock CLOCK = new MonotonicClock();

  private FederationMembershipStateStore membershipStateStore =
      getMembershipStateStore();
  private FederationApplicationHomeSubClusterStore applicationStateStore =
      getApplicationStateStore();

  @Before
  public void before() throws IOException {
    clearMembership();
    clearApplications();
  }

  // Test FederationMembershipStateStore;

  @Test
  public void testRegisterSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    SubClusterRegisterResponse result = membershipStateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));
    Map<SubClusterId, SubClusterInfo> membership = getMembership();

    Assert.assertNotNull(membership.get(subClusterId));
    Assert.assertNotNull(result);
    Assert.assertEquals(subClusterInfo, membership.get(subClusterId));
  }

  @Test
  public void testDeregisterSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    membershipStateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    SubClusterDeregisterRequest deregisterRequest = SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED);

    membershipStateStore.deregisterSubCluster(deregisterRequest);

    Map<SubClusterId, SubClusterInfo> membership = getMembership();
    Assert.assertNotNull(membership.get(subClusterId));
    Assert.assertEquals(membership.get(subClusterId).getState(),
        SubClusterState.SC_UNREGISTERED);
  }

  @Test
  public void testDeregisterSubClusterUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");

    SubClusterDeregisterRequest deregisterRequest = SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED);
    try {
      membershipStateStore.deregisterSubCluster(deregisterRequest);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().startsWith("SubCluster SC not found"));
    }
  }

  @Test
  public void testGetSubClusterInfo() throws Exception {

    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    membershipStateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    Assert.assertEquals(subClusterInfo,
        membershipStateStore.getSubCluster(request).getSubClusterInfo());
  }

  @Test
  public void testGetSubClusterInfoUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);

    try {
      membershipStateStore.getSubCluster(request).getSubClusterInfo();
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Subcluster SC does not exist"));
    }
  }

  @Test
  public void testGetAllSubClustersInfo() throws Exception {

    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    SubClusterInfo subClusterInfo1 = createSubClusterInfo(subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    SubClusterInfo subClusterInfo2 = createSubClusterInfo(subClusterId2);

    membershipStateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo1));
    membershipStateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo2));

    membershipStateStore.subClusterHeartbeat(SubClusterHeartbeatRequest
        .newInstance(subClusterId1, SubClusterState.SC_RUNNING, ""));
    membershipStateStore.subClusterHeartbeat(SubClusterHeartbeatRequest
        .newInstance(subClusterId2, SubClusterState.SC_UNHEALTHY, ""));

    Assert.assertTrue(membershipStateStore
        .getSubClusters(GetSubClustersInfoRequest.newInstance(true))
        .getSubClusters().contains(subClusterInfo1));
    Assert.assertFalse(membershipStateStore
        .getSubClusters(GetSubClustersInfoRequest.newInstance(true))
        .getSubClusters().contains(subClusterInfo2));

    Assert.assertTrue(membershipStateStore
        .getSubClusters(GetSubClustersInfoRequest.newInstance(false))
        .getSubClusters().contains(subClusterInfo1));
    Assert.assertTrue(membershipStateStore
        .getSubClusters(GetSubClustersInfoRequest.newInstance(false))
        .getSubClusters().contains(subClusterInfo2));
  }

  @Test
  public void testSubClusterHeartbeat() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    membershipStateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    SubClusterHeartbeatRequest heartbeatRequest = SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_RUNNING, "cabability");
    membershipStateStore.subClusterHeartbeat(heartbeatRequest);

    Map<SubClusterId, SubClusterInfo> membership = getMembership();
    Assert.assertEquals(membership.get(subClusterId).getState(),
        SubClusterState.SC_RUNNING);
    Assert.assertNotNull(membership.get(subClusterId).getLastHeartBeat());
  }

  @Test
  public void testSubClusterHeartbeatUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterHeartbeatRequest heartbeatRequest = SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_RUNNING, "cabability");

    try {
      membershipStateStore.subClusterHeartbeat(heartbeatRequest);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Subcluster SC does not exist; cannot heartbeat"));
    }
  }

  private SubClusterInfo createSubClusterInfo(SubClusterId subClusterId) {

    String amRMAddress = "1.2.3.4:1";
    String clientRMAddress = "1.2.3.4:2";
    String rmAdminAddress = "1.2.3.4:3";
    String webAppAddress = "1.2.3.4:4";

    return SubClusterInfo.newInstance(subClusterId, amRMAddress,
        clientRMAddress, rmAdminAddress, webAppAddress, SubClusterState.SC_NEW,
        CLOCK.getTime(), "cabability");
  }

  // Test FederationApplicationHomeSubClusterStore

  @Test
  public void testAddApplicationHomeSubClusterMap() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);

    AddApplicationHomeSubClusterRequest request =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    AddApplicationHomeSubClusterResponse response =
        applicationStateStore.addApplicationHomeSubClusterMap(request);

    Map<ApplicationId, SubClusterId> applications = getApplications();
    Assert.assertNotNull(response);

    Assert.assertNotNull(applications.get(appId));
    Assert.assertEquals(subClusterId, applications.get(appId));

  }

  @Test
  public void testAddApplicationHomeSubClusterMapAppAlreadyExists()
      throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");

    ApplicationHomeSubCluster ahsc1 =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);
    ApplicationHomeSubCluster ahsc2 =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId2);

    applicationStateStore.addApplicationHomeSubClusterMap(
        AddApplicationHomeSubClusterRequest.newInstance(ahsc1));

    try {
      applicationStateStore.addApplicationHomeSubClusterMap(
          AddApplicationHomeSubClusterRequest.newInstance(ahsc2));
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " already exists"));
    }

    Map<ApplicationId, SubClusterId> applications = getApplications();
    Assert.assertEquals(subClusterId1, applications.get(appId));

  }

  @Test
  public void testDeleteApplicationHomeSubClusterMap() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);

    AddApplicationHomeSubClusterRequest addRequest =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    applicationStateStore.addApplicationHomeSubClusterMap(addRequest);

    DeleteApplicationHomeSubClusterRequest delRequest =
        DeleteApplicationHomeSubClusterRequest.newInstance(appId);

    DeleteApplicationHomeSubClusterResponse response =
        applicationStateStore.deleteApplicationHomeSubClusterMap(delRequest);

    Map<ApplicationId, SubClusterId> applications = getApplications();

    Assert.assertNotNull(response);
    Assert.assertFalse(applications.containsKey(appId));
  }

  @Test
  public void testDeleteApplicationHomeSubClusterMapUnknownApp()
      throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    DeleteApplicationHomeSubClusterRequest delRequest =
        DeleteApplicationHomeSubClusterRequest.newInstance(appId);

    try {
      applicationStateStore.deleteApplicationHomeSubClusterMap(delRequest);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " not found"));
    }
  }

  @Test
  public void testGetApplicationHomeSubClusterMap() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId);

    AddApplicationHomeSubClusterRequest addRequest =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    applicationStateStore.addApplicationHomeSubClusterMap(addRequest);

    GetApplicationHomeSubClusterRequest getRequest =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    Assert.assertNotNull(appId);

    GetApplicationHomeSubClusterResponse result =
        applicationStateStore.getApplicationHomeSubClusterMap(getRequest);

    Assert.assertEquals(appId,
        result.getApplicationHomeSubCluster().getApplicationId());
    Assert.assertEquals(subClusterId,
        result.getApplicationHomeSubCluster().getHomeSubCluster());
  }

  @Test
  public void testGetApplicationHomeSubClusterMapUnknownApp() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    GetApplicationHomeSubClusterRequest request =
        GetApplicationHomeSubClusterRequest.newInstance(appId);

    try {
      applicationStateStore.getApplicationHomeSubClusterMap(request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " not found"));
    }
  }

  @Test
  public void testGetApplicationsHomeSubClusterMap() throws Exception {
    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    ApplicationHomeSubCluster ahsc1 =
        ApplicationHomeSubCluster.newInstance(appId1, subClusterId1);

    AddApplicationHomeSubClusterRequest addRequest1 =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc1);
    applicationStateStore.addApplicationHomeSubClusterMap(addRequest1);

    ApplicationId appId2 = ApplicationId.newInstance(1, 2);
    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    ApplicationHomeSubCluster ahsc2 =
        ApplicationHomeSubCluster.newInstance(appId2, subClusterId2);

    AddApplicationHomeSubClusterRequest addRequest2 =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc2);
    applicationStateStore.addApplicationHomeSubClusterMap(addRequest2);

    GetApplicationsHomeSubClusterRequest getRequest =
        GetApplicationsHomeSubClusterRequest.newInstance();

    GetApplicationsHomeSubClusterResponse result =
        applicationStateStore.getApplicationsHomeSubClusterMap(getRequest);

    Assert.assertEquals(2, result.getAppsHomeSubClusters().size());
    Assert.assertTrue(result.getAppsHomeSubClusters().contains(ahsc1));
    Assert.assertTrue(result.getAppsHomeSubClusters().contains(ahsc2));
  }

  @Test
  public void testUpdateApplicationHomeSubClusterMap() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);

    AddApplicationHomeSubClusterRequest addRequest =
        AddApplicationHomeSubClusterRequest.newInstance(ahsc);
    applicationStateStore.addApplicationHomeSubClusterMap(addRequest);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");

    ApplicationHomeSubCluster ahscUpdate =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId2);

    UpdateApplicationHomeSubClusterRequest updateRequest =
        UpdateApplicationHomeSubClusterRequest.newInstance(ahscUpdate);

    UpdateApplicationHomeSubClusterResponse response =
        applicationStateStore.updateApplicationHomeSubClusterMap(updateRequest);

    Map<ApplicationId, SubClusterId> applications = getApplications();
    Assert.assertNotNull(response);

    Assert.assertNotNull(applications.get(appId));
    Assert.assertEquals(subClusterId2, applications.get(appId));
  }

  @Test
  public void testUpdateApplicationHomeSubClusterMapUnknownApp()
      throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    ApplicationHomeSubCluster ahsc =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);

    UpdateApplicationHomeSubClusterRequest updateRequest =
        UpdateApplicationHomeSubClusterRequest.newInstance(ahsc);

    try {
      applicationStateStore.updateApplicationHomeSubClusterMap((updateRequest));
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Application " + appId.toString() + " not found"));
    }
  }

  protected abstract FederationMembershipStateStore getMembershipStateStore();

  protected abstract FederationApplicationHomeSubClusterStore getApplicationStateStore();

  protected abstract Map<SubClusterId, SubClusterInfo> getMembership();

  protected abstract void clearMembership();

  protected abstract Map<ApplicationId, SubClusterId> getApplications();

  protected abstract void clearApplications();

}
