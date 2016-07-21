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

import java.io.IOException;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterState;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.junit.Assert;
import org.junit.Test;

public class TestFederationInMemoryMembershipState {

  static final Logger LOG =
      LoggerFactory.getLogger(TestFederationInMemoryMembershipState.class);
  private static final FederationInMemoryMembershipState stateStore =
      new FederationInMemoryMembershipState();

  private static final MonotonicClock clock = new MonotonicClock();

  @Before
  public void before() throws IOException {
    stateStore.clearMembershipTable();

  }

  @Test
  public void testRegisterSubCluster() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC");
    FederationSubClusterInfo subClusterInfo = getSubClusterInfo(subClusterId);

    FederationSubClusterId result =
        stateStore.registerSubCluster(subClusterInfo);

    Map<FederationSubClusterId, FederationSubClusterInfo> membership =
        stateStore.getMembershipTable();
    Assert.assertNotNull(membership.get(subClusterId));
    Assert.assertEquals(subClusterId, result);
    Assert.assertEquals(subClusterInfo, membership.get(subClusterId));
  }

  @Test
  public void testDeregisterSubCluster() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC");
    FederationSubClusterInfo subClusterInfo = getSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(subClusterInfo);

    FederationSubClusterDeregisterRequest deregisterRequest =
        FederationSubClusterDeregisterRequest.newInstance(subClusterId,
            FederationSubClusterState.SC_DEREGISTERED);

    stateStore.deregisterSubCluster(deregisterRequest);

    Map<FederationSubClusterId, FederationSubClusterInfo> membership =
        stateStore.getMembershipTable();
    Assert.assertNotNull(membership.get(subClusterId));

    FederationSubClusterInfo expected = getSubClusterInfo(subClusterId);
    expected.setState(FederationSubClusterState.SC_DEREGISTERED);

    compareSubClusterInfo(expected, membership.get(subClusterId));

  }

  @Test
  public void testGetSubClusterInfo() throws Exception {

    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC");
    FederationSubClusterInfo subClusterInfo = getSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(subClusterInfo);

    Assert.assertEquals(subClusterInfo,
        stateStore.getSubClusterInfo(subClusterId));

  }

  @Test
  public void testGetAllSubClustersInfo() throws Exception {

    FederationSubClusterId subClusterId1 =
        FederationSubClusterId.newInstance("SC1");
    FederationSubClusterInfo subClusterInfo1 = getSubClusterInfo(subClusterId1);

    FederationSubClusterId subClusterId2 =
        FederationSubClusterId.newInstance("SC2");
    FederationSubClusterInfo subClusterInfo2 = getSubClusterInfo(subClusterId2);

    stateStore.registerSubCluster(subClusterInfo1);
    stateStore.registerSubCluster(subClusterInfo2);

    FederationSubClusterInfo subClusterInfo1Copy =
        getSubClusterInfo(subClusterId1);

    subClusterInfo1Copy.setState(FederationSubClusterState.SC_RUNNING);

    stateStore.subClusterHeartbeat(subClusterInfo1Copy);

    Assert.assertTrue(
        stateStore.getAllSubClustersInfo().containsKey(subClusterId1));
    Assert.assertFalse(
        stateStore.getAllSubClustersInfo().containsKey(subClusterId2));

    // Only return Subcluster 1, since subcluster 2 is in SC_NEW state.
    Assert.assertEquals(1, stateStore.getAllSubClustersInfo().size());

    compareSubClusterInfo(subClusterInfo1Copy,
        stateStore.getAllSubClustersInfo().get(subClusterId1));
  }

  @Test
  public void testSubClusterHeartbeat() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC");
    FederationSubClusterInfo subClusterInfo = getSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(subClusterInfo);

    FederationSubClusterInfo subClusterInfoCopy =
        getSubClusterInfo(subClusterId);

    subClusterInfoCopy.setState(FederationSubClusterState.SC_RUNNING);

    stateStore.subClusterHeartbeat(subClusterInfoCopy);

    Map<FederationSubClusterId, FederationSubClusterInfo> membership =
        stateStore.getMembershipTable();

    Assert.assertEquals(membership.get(subClusterId).getState(),
        FederationSubClusterState.SC_RUNNING);
    Assert.assertNotNull(membership.get(subClusterId).getLastHeartBeat());

  }

  private FederationSubClusterInfo getSubClusterInfo(
      FederationSubClusterId subClusterId) {

    String amRMAddress = "1.2.3.4:1";
    String clientRMAddress = "1.2.3.4:2";
    String rmAdminAddress = "1.2.3.4:3";
    String webAppAddress = "1.2.3.4:4";

    return FederationSubClusterInfo.newInstance(subClusterId, amRMAddress,
        clientRMAddress, rmAdminAddress, webAppAddress,
        FederationSubClusterState.SC_NEW, clock.getTime(), "cabability");
  }

  // compare FederationSubClusterInfo equality, ignoring timestamps private
  void compareSubClusterInfo(FederationSubClusterInfo expected,
      FederationSubClusterInfo actual) {
    Assert.assertEquals(expected.getSubClusterId(), actual.getSubClusterId());
    Assert.assertEquals(expected.getState(), actual.getState());

    Assert.assertEquals(expected.getAMRMAddress(), actual.getAMRMAddress());
    Assert.assertEquals(expected.getCapability(), actual.getCapability());
    Assert.assertEquals(expected.getClientRMAddress(),
        actual.getClientRMAddress());
    Assert.assertEquals(expected.getRMAdminAddress(),
        actual.getRMAdminAddress());
    Assert.assertEquals(expected.getWebAppAddress(), actual.getWebAppAddress());
  }

}
