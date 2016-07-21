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
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.api.records.FederationApplicationInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationDeleteApplicationRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationDeleteApplicationResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationInsertNewApplicationRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationInsertNewApplicationResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.apache.hadoop.yarn.server.federation.api.records.FederationUpdateApplicationRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationUpdateApplicationResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFederationInMemoryApplicationState {
  static final Logger LOG =
      LoggerFactory.getLogger(TestFederationInMemoryApplicationState.class);
  private static final FederationInMemoryApplicationState stateStore =
      new FederationInMemoryApplicationState();

  @Before
  public void before() throws IOException {
    stateStore.clearApplicationsTable();

  }

  /*
   * Test Application State
   */

  @Test
  public void testInsertNewApplication() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC");
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    FederationApplicationInfo appInfo =
        FederationApplicationInfo.newInstance(appId, subClusterId);

    FederationInsertNewApplicationRequest request =
        FederationInsertNewApplicationRequest.newInstance(appInfo);
    FederationInsertNewApplicationResponse result =
        stateStore.insertNewApplication(request);
    Assert.assertNotNull(result);

    Map<ApplicationId, FederationApplicationInfo> applications =
        stateStore.getApplicationsTable();

    Assert.assertTrue(applications.containsKey(appId));
    Assert.assertEquals(appInfo, applications.get(appId));
  }

  @Test
  public void testUpdateApplication() throws Exception {
    FederationSubClusterId subClusterId1 =
        FederationSubClusterId.newInstance("SC1");
    FederationSubClusterId subClusterId2 =
        FederationSubClusterId.newInstance("SC2");

    ApplicationId appId = ApplicationId.newInstance(1, 1);
    FederationApplicationInfo appInfo =
        FederationApplicationInfo.newInstance(appId, subClusterId1);

    FederationApplicationInfo appInfoCopy =
        FederationApplicationInfo.newInstance(appId, subClusterId2);

    FederationInsertNewApplicationRequest request =
        FederationInsertNewApplicationRequest.newInstance(appInfo);
    stateStore.insertNewApplication(request);

    FederationUpdateApplicationResponse result = stateStore.updateApplication(
        FederationUpdateApplicationRequest.newInstance(appInfoCopy));
    Assert.assertNotNull(result);

    Map<ApplicationId, FederationApplicationInfo> applications =
        stateStore.getApplicationsTable();
    Assert.assertNotNull(applications.get(appId));
    Assert.assertEquals(subClusterId2,
        applications.get(appId).getHomeSubCluster());

  }

  @Test
  public void testGetApplicationInfo() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC");
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    FederationApplicationInfo appInfo =
        FederationApplicationInfo.newInstance(appId, subClusterId);
    FederationInsertNewApplicationRequest request =
        FederationInsertNewApplicationRequest.newInstance(appInfo);

    stateStore.insertNewApplication(request);

    Assert.assertEquals(appInfo, stateStore.getApplicationInfo(appId));

  }

  @Test
  public void testGetAllApplicationsInfo() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC1");
    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    FederationApplicationInfo appInfo1 =
        FederationApplicationInfo.newInstance(appId1, subClusterId);
    FederationInsertNewApplicationRequest request1 =
        FederationInsertNewApplicationRequest.newInstance(appInfo1);

    ApplicationId appId2 = ApplicationId.newInstance(1, 2);
    FederationApplicationInfo appInfo2 =
        FederationApplicationInfo.newInstance(appId2, subClusterId);
    FederationInsertNewApplicationRequest request2 =
        FederationInsertNewApplicationRequest.newInstance(appInfo2);

    stateStore.insertNewApplication(request1);
    stateStore.insertNewApplication(request2);

    Map<ApplicationId, FederationApplicationInfo> applications =
        stateStore.getApplicationsTable();

    Assert.assertEquals(2, applications.size());
    Assert.assertEquals(appInfo1, applications.get(appId1));
    Assert.assertEquals(appInfo2, applications.get(appId2));
  }

  @Test
  public void testDeleteApplication() throws Exception {
    FederationSubClusterId subClusterId =
        FederationSubClusterId.newInstance("SC1");
    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    FederationApplicationInfo appInfo1 =
        FederationApplicationInfo.newInstance(appId1, subClusterId);
    FederationInsertNewApplicationRequest request1 =
        FederationInsertNewApplicationRequest.newInstance(appInfo1);

    stateStore.insertNewApplication(request1);

    FederationDeleteApplicationRequest request =
        FederationDeleteApplicationRequest.newInstance(appId1);
    FederationDeleteApplicationResponse response =
        stateStore.deleteApplication(request);

    Assert.assertNotNull(response);
    Map<ApplicationId, FederationApplicationInfo> applications =
        stateStore.getApplicationsTable();

    Assert.assertEquals(0, applications.size());

  }
}
