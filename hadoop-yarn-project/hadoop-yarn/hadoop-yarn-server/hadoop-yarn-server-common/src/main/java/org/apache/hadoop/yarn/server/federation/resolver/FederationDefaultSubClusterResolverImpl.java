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

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Default simple sub-cluster resolver class.
 *
 * This class expects a two-column comma separated file, specified in
 * yarn.federation.machine-list. Each line of the file should be of the format:
 *
 * nodeName, subClusterId
 *
 * Lines that do not follow this format will be ignored. This resolver only
 * loads the file when load() is explicitly called; it will not react to changes
 * to the file.
 *
 * It is case-insensitive (on the node name) and ignores leading/trailing
 * whitespace.
 *
 */
public class FederationDefaultSubClusterResolverImpl
    extends FederationAbstractSubClusterResolver
    implements FederationSubClusterResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationDefaultSubClusterResolverImpl.class);
  private Configuration conf;

  // Index of the node hostname in the machine info file.

  private static final int NODE_NAME_INDEX = 0;

  // Index of the sub-cluster ID in the machine info file.
  private static final int SUBCLUSTER_ID_INDEX = 1;

  private static final int RACK_NAME_INDEX = 2;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public FederationSubClusterId getSubClusterForNode(String nodename)
      throws YarnException {
    return super.getSubClusterForNode(nodename.toUpperCase());
  }

  @Override
  public void load() {
    String fileName =
        this.conf.get(YarnConfiguration.FEDERATION_MACHINE_LIST, "");

    try {
      if (fileName == null || fileName.trim().length() == 0) {
        LOG.info(
            "The machine list file path is not specified in the configuration");
        return;
      }

      Path file = null;
      BufferedReader reader = null;

      try {
        file = Paths.get(fileName);
      } catch (InvalidPathException e) {
        LOG.info("The configured machine list file path {} does not exist",
            fileName);
        return;
      }

      try {
        reader = Files.newBufferedReader(file, Charset.defaultCharset());
        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split(",");
          if (tokens.length == 3) {

            String nodeName = tokens[NODE_NAME_INDEX].trim().toUpperCase();
            FederationSubClusterId subClusterId = FederationSubClusterId
                .newInstance(tokens[SUBCLUSTER_ID_INDEX].trim());
            String rackName = tokens[RACK_NAME_INDEX].trim().toUpperCase();

            if (LOG.isDebugEnabled()) {
              LOG.debug("Loading node into resolver: {} --> {}", nodeName,
                  subClusterId);
              LOG.debug("Loading rack into resolver: {} --> {} ", rackName,
                  subClusterId);
            }

            this.nodeToSubCluster.put(nodeName, subClusterId);
            loadRackToSubCluster(rackName, subClusterId);
          } else {
            LOG.warn("Skipping malformed line in machine list: " + line);
          }
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
      LOG.info("Successfully loaded file {}", fileName);

    } catch (Exception e) {
      LOG.error("Failed to parse file " + fileName, e);
    }
  }

  private void loadRackToSubCluster(String rackName,
      FederationSubClusterId subClusterId) {
    String rackNameUpper = rackName.toUpperCase();

    if (!this.rackToSubClusters.containsKey(rackNameUpper)) {
      this.rackToSubClusters.put(rackNameUpper,
          new HashSet<FederationSubClusterId>());
    }

    this.rackToSubClusters.get(rackNameUpper).add(subClusterId);

  }

  @Override
  public Set<FederationSubClusterId> getSubClustersForRack(String rackname)
      throws YarnException {
    return super.getSubClustersForRack(rackname.toUpperCase());
  }
}
