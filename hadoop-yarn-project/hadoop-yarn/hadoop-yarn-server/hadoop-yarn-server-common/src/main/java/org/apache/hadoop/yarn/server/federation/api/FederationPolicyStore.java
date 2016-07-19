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

package org.apache.hadoop.yarn.server.federation.api;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationGetRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationGetResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationSetRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationSetResponse;

/**
 * The FederationPolicyStore provides a key-value interface to the policies
 * configured for the active queues in the federation system. The
 * {@link FederationPolicyConfiguration} for each queue indicates the
 * distribution of his resources across the various federated subclusters.
 */
public interface FederationPolicyStore {

  /**
   * Perform any initialization operation(s) of the PolicyStore.
   *
   * @param conf the cluster configuration
   * @throws YarnException if initialization fails
   */
  public void init(Configuration conf) throws YarnException;

  /**
   * Get the policy information as {@link FederationPolicyConfiguration}, which
   * represents the distribution of his resources across subclusters for the
   * specified queue.
   * 
   * @param request the queue whose {@link FederationPolicyConfiguration} is
   *          required
   * @return the {@link FederationPolicyConfiguration} for the specified queue
   * @throws YarnException
   */
  public FederationPolicyConfigurationGetResponse getPolicyConfigurationForQueue(
      FederationPolicyConfigurationGetRequest request) throws YarnException;

  /**
   * Set the policy information as {@link FederationPolicyConfiguration}, which
   * represents the distribution of his resources across subclusters for the
   * specified queue.
   *
   * @param request the {@link FederationPolicyConfiguration} with the
   *          corresponding queue
   * @return response empty on successfully updating the
   *         {@link FederationPolicyConfiguration} for the specified queue
   * @throws YarnException
   */
  public FederationPolicyConfigurationSetResponse setPolicyConfigurationForQueue(
      FederationPolicyConfigurationSetRequest request) throws YarnException;

  /**
   * Get the policies that is represented as
   * {@link FederationPolicyConfiguration} for all currently active queues in
   * the system.
   * 
   * @return the policies for all currently active queues in the system
   * @throws YarnException
   */
  public Map<String, FederationPolicyConfiguration> getAllPolicies()
      throws YarnException;

  /**
   * Perform any cleanup operation(s) of the PolicyStore.
   *
   * @throws Exception if cleanup fails
   */
  public void close() throws Exception;

}
