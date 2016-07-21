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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * UpdateFederationPolicyRequest is a request to the
 * {@code FederationPolicyStore} to update the information about the
 * distribution of resources across sub-clusters, i.e. the
 * {@link FederationPolicyConfiguration} for the specified queue.
 */
@Public
@Unstable
public abstract class FederationPolicyConfigurationSetRequest {
  @Private
  @Unstable
  public static FederationPolicyConfigurationSetRequest newInstance(
      String queue, FederationPolicyConfiguration policy) {
    FederationPolicyConfigurationSetRequest request =
        Records.newRecord(FederationPolicyConfigurationSetRequest.class);
    request.setQueue(queue);
    request.setPolicy(policy);
    return request;
  }

  /**
   * Get the name of the queue whose policy is required.
   *
   * @return the name of the queue
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * Sets the name of the queue whose policy is required.
   *
   * @param queueName the name of the queue
   */
  @Private
  @Unstable
  public abstract void setQueue(String queueName);

  /**
   * Get the policy which represents the distribution of his resources across
   * sub-clusters.
   *
   * @return the policy for the specified queue
   */
  @Public
  @Unstable
  public abstract FederationPolicyConfiguration getPolicy();

  /**
   * Sets the policy which represents the distribution of his resources across
   * sub-clusters.
   *
   * @param policy the policy for the specified queue
   */
  @Private
  @Unstable
  public abstract void setPolicy(FederationPolicyConfiguration policy);
}