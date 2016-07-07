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

package org.apache.hadoop.yarn.server.federation.api.records;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.BasePBImplRecordsTest;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.*;
import org.apache.hadoop.yarn.server.federation.api.records.impl.pb.*;
import org.apache.hadoop.yarn.server.records.Version;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for federation protocol records.
 */
public class TestFederationProtocolRecords extends BasePBImplRecordsTest {

  @BeforeClass
  public static void setup() throws Exception {
    generateByNewInstance(ApplicationId.class);
    generateByNewInstance(Version.class);
    generateByNewInstance(FederationSubClusterId.class);
  }

  @Test
  public void testFederationSubClusterId() throws Exception {
    validatePBImplRecord(FederationSubClusterIdPBImpl.class,
        FederationSubClusterIdProto.class);
  }

  @Test
  public void testFederationSubClusterInfo() throws Exception {
    validatePBImplRecord(FederationSubClusterInfoPBImpl.class,
        FederationSubClusterInfoProto.class);
  }

  @Test
  public void testFederationSubClusterHeartbeatResponse() throws Exception {
    validatePBImplRecord(FederationSubClusterHeartbeatResponsePBImpl.class,
        FederationSubClusterHeartbeatResponseProto.class);
  }

  @Test
  public void testFederationSubClusterDeregisterRequest() throws Exception {
    validatePBImplRecord(FederationSubClusterDeregisterRequestPBImpl.class,
        FederationSubClusterDeregisterRequestProto.class);
  }

  @Test
  public void testFederationSubClusterDeregisterResponse() throws Exception {
    validatePBImplRecord(FederationSubClusterDeregisterResponsePBImpl.class,
        FederationSubClusterDeregisterResponseProto.class);
  }

  @Test
  public void testFederationMembershipStateVersion() throws Exception {
    validatePBImplRecord(FederationMembershipStateVersionPBImpl.class,
        FederationMembershipStateVersionProto.class);
  }

}