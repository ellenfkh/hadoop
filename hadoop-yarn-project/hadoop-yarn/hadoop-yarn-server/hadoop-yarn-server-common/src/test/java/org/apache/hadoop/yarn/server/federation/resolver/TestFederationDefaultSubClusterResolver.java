/**
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

package org.apache.hadoop.yarn.server.federation.resolver;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.junit.Assert;
import org.junit.Test;

public class TestFederationDefaultSubClusterResolver {
  private static YarnConfiguration conf;
  private static FederationSubClusterResolver resolver;
  private static final Log LOG =
      LogFactory.getLog(TestFederationDefaultSubClusterResolver.class);

  public static void setUp() {
    conf = new YarnConfiguration();
    resolver = new FederationDefaultSubClusterResolverImpl();

    URL url =
        Thread.currentThread().getContextClassLoader().getResource("nodes");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'nodes' dummy file in classpath");
    }

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, url.getPath());
    resolver.setConf(conf);
    resolver.load();
  }

  private void setUpMalformedFile() {
    conf = new YarnConfiguration();
    resolver = new FederationDefaultSubClusterResolverImpl();

    URL url = Thread.currentThread().getContextClassLoader()
        .getResource("nodes-malformed");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'nodes-malformed' dummy file in classpath");
    }

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, url.getPath());
    resolver.setConf(conf);
    resolver.load();
  }

  private void setUpNonExistentFile() {
    conf = new YarnConfiguration();
    resolver = new FederationDefaultSubClusterResolverImpl();

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, "fileDoesNotExist");
    resolver.setConf(conf);
    resolver.load();
  }

  @Test
  public void testGetSubClusterForNode() throws YarnException {
    LOG.info("Test: getSubClusterForNode with a good machine list file");

    setUp();

    // All lowercase, no whitespace in machine list file
    Assert.assertEquals(FederationSubClusterId.newInstance("subcluster1"),
        resolver.getSubClusterForNode("node1"));
    // Leading and trailing whitespace in machine list file
    Assert.assertEquals(FederationSubClusterId.newInstance("subcluster2"),
        resolver.getSubClusterForNode("node2"));
    // Node name capitalization in machine list file
    Assert.assertEquals(FederationSubClusterId.newInstance("subcluster3"),
        resolver.getSubClusterForNode("node3"));

    try {
      resolver.getSubClusterForNode("nodeDoesNotExist");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }
  }

  @Test
  public void testGetSubClusterForNodeMalformedFile() throws YarnException {
    LOG.info("Test: getSubClusterForNode with a malformed machine list file");

    setUpMalformedFile();

    try {
      resolver.getSubClusterForNode("node1");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }

    try {
      resolver.getSubClusterForNode("node2");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }

    Assert.assertEquals(FederationSubClusterId.newInstance("subcluster3"),
        resolver.getSubClusterForNode("node3"));

    try {
      resolver.getSubClusterForNode("nodeDoesNotExist");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }
  }

  @Test
  public void testGetSubClusterForNodeNoFile() throws YarnException {
    LOG.info("Test: getSubClusterForNode with a nonexistent machine list file");

    setUpNonExistentFile();

    try {
      resolver.getSubClusterForNode("node1");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }
  }

  @Test
  public void testGetSubClustersForRack() throws YarnException {
    LOG.info("Test: getSubClustersForRack with a good machine list file");

    setUp();

    Set<FederationSubClusterId> rack1Expected =
        new HashSet<FederationSubClusterId>();
    rack1Expected.add(FederationSubClusterId.newInstance("subcluster1"));
    rack1Expected.add(FederationSubClusterId.newInstance("subcluster2"));

    Set<FederationSubClusterId> rack2Expected =
        new HashSet<FederationSubClusterId>();
    rack2Expected.add(FederationSubClusterId.newInstance("subcluster3"));

    // Two subclusters have nodes in rack1
    Assert.assertEquals(rack1Expected, resolver.getSubClustersForRack("rack1"));

    // Two nodes are in rack2, but both belong to subcluster3
    Assert.assertEquals(rack2Expected, resolver.getSubClustersForRack("rack2"));

    try {
      resolver.getSubClustersForRack("rackDoesNotExist");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().startsWith("Cannot resolve rack"));
    }
  }

  @Test
  public void testGetSubClustersForRackNoFile() throws YarnException {
    LOG.info(
        "Test: getSubClustersForRack with a nonexistent machine list file");

    setUpNonExistentFile();

    try {
      resolver.getSubClustersForRack("rack1");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().startsWith("Cannot resolve rack"));
    }
  }
}
