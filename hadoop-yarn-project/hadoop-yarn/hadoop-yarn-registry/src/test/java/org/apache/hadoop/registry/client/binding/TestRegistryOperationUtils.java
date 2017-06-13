/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.registry.client.binding;

import org.apache.hadoop.registry.client.api.records.ApplicationServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ContainerServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.CoreServiceRecordKey;
import org.apache.hadoop.registry.client.api.records.ServiceRecordKey;
import org.apache.hadoop.registry.client.exceptions.InvalidRegistryKeyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link RegistryUtils} class
 */
public class TestRegistryOperationUtils extends Assert {

  @Test
  public void testUsernameExtractionEnvVarOverrride() throws Throwable {
    String whoami = RegistryUtils.getCurrentUsernameUnencoded("drwho");
    assertEquals("drwho", whoami);

  }

  @Test
  public void testUsernameExtractionCurrentuser() throws Throwable {
    String whoami = RegistryUtils.getCurrentUsernameUnencoded("");
    String ugiUser = UserGroupInformation.getCurrentUser().getShortUserName();
    assertEquals(ugiUser, whoami);
  }

  @Test
  public void testShortenUsername() throws Throwable {
    assertEquals("hbase",
        RegistryUtils.convertUsername("hbase@HADOOP.APACHE.ORG"));
    assertEquals("hbase",
        RegistryUtils.convertUsername("hbase/localhost@HADOOP.APACHE.ORG"));
    assertEquals("hbase", RegistryUtils.convertUsername("hbase"));
    assertEquals("hbase user", RegistryUtils.convertUsername("hbase user"));
  }

  @Test
  public void testGetZKPathForServiceRecordKey()
      throws InvalidRegistryKeyException {
    ServiceRecordKey coreKey =
        new CoreServiceRecordKey("serviceClass", "instanceName");
    ServiceRecordKey appKey =
        new ApplicationServiceRecordKey("username", "serviceClass", "appId");
    ServiceRecordKey containerKey = new ContainerServiceRecordKey("username",
        "serviceClass", "appId", "containerId");

    String coreKeyPath = RegistryUtils.getPathForServiceRecordKey(coreKey);
    String appKeyPath = RegistryUtils.getPathForServiceRecordKey(appKey);
    String containerKeyPath =
        RegistryUtils.getPathForServiceRecordKey(containerKey);

    Assert.assertEquals("/core/serviceClass/instanceName", coreKeyPath);
    Assert.assertEquals("/user/username/serviceClass/appId", appKeyPath);
    Assert.assertEquals("/user/username/serviceClass/appId/containerId",
        containerKeyPath);
  }

  @Test
  public void testGetServiceRecordKeyFromZKPath()
      throws InvalidRegistryKeyException {
    ServiceRecordKey coreKey = RegistryUtils
        .getServiceRecordKeyFromZKPath("/core/serviceClass/instanceName");
    ServiceRecordKey appKey = RegistryUtils
        .getServiceRecordKeyFromZKPath("/user/username/serviceClass/appId");
    ServiceRecordKey containerKey = RegistryUtils.getServiceRecordKeyFromZKPath(
        "/user/username/serviceClass/appId/containerId");

    Assert.assertEquals(
        new CoreServiceRecordKey("serviceClass", "instanceName"), coreKey);
    Assert.assertEquals(
        new ApplicationServiceRecordKey("username", "serviceClass", "appId"),
        appKey);
    Assert.assertEquals(new ContainerServiceRecordKey("username",
        "serviceClass", "appId", "containerId"), containerKey);
  }

  @Test
  public void testGetServiceRecordKeyFromArgs() {
    // TODO

  }
}
