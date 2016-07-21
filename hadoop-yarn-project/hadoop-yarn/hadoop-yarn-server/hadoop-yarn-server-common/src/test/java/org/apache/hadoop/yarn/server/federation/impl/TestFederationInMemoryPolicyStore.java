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
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationGetRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationSetRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationSetResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFederationInMemoryPolicyStore {
  static final Logger LOG =
      LoggerFactory.getLogger(TestFederationInMemoryPolicyStore.class);
  private static final FederationInMemoryPolicyStore policyStore =
      new FederationInMemoryPolicyStore();

  @Before
  public void before() throws IOException {
    policyStore.clearPoliciesTable();
  }

  @Test
  public void testSetPolicyConfigurationForQueue() throws Exception {
    FederationPolicyConfiguration policy = FederationPolicyConfiguration
        .newInstance("policyType", ByteBuffer.allocate(1));

    FederationPolicyConfigurationSetResponse response = policyStore
        .setPolicyConfigurationForQueue(FederationPolicyConfigurationSetRequest
            .newInstance("queue", policy));
    Assert.assertNotNull(response);
    Assert.assertEquals(policyStore.getPoliciesTable().get("queue"), policy);

  }

  @Test
  public void testGetPolicyConfigurationForQueue() throws Exception {
    FederationPolicyConfiguration policy = FederationPolicyConfiguration
        .newInstance("policyType", ByteBuffer.allocate(1));

    policyStore.setPolicyConfigurationForQueue(
        FederationPolicyConfigurationSetRequest.newInstance("queue", policy));

    Assert.assertEquals(
        policyStore.getPolicyConfigurationForQueue(
            FederationPolicyConfigurationGetRequest.newInstance("queue")),
        policy);

  }

  @Test
  public void testGetAllPolicies() throws Exception {
    FederationPolicyConfiguration policy1 = FederationPolicyConfiguration
        .newInstance("policy1Type", ByteBuffer.allocate(1));
    FederationPolicyConfiguration policy2 = FederationPolicyConfiguration
        .newInstance("policy2Type", ByteBuffer.allocate(1));

    policyStore.setPolicyConfigurationForQueue(
        FederationPolicyConfigurationSetRequest.newInstance("queue1", policy1));
    policyStore.setPolicyConfigurationForQueue(
        FederationPolicyConfigurationSetRequest.newInstance("queue2", policy2));

    Map<String, FederationPolicyConfiguration> response =
        policyStore.getAllPolicies();
    Assert.assertEquals(2, response.size());

    Assert.assertTrue(response.equals(policyStore.getPoliciesTable()));

  }
}
