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
 * FederationPolicyConfigurationGetResponse contains the answer from the {@code
 * FederationPolicyStore} to a request to get the information about {@link
 * FederationPolicyConfiguration}, which represents the distribution of his resources across
 * sub-clusters.
 */
@Public
@Unstable
public abstract class FederationPolicyConfigurationGetResponse {

  @Private
  @Unstable
  public FederationPolicyConfigurationGetResponse newInstance(FederationPolicyConfiguration policy) {
    FederationPolicyConfigurationGetResponse response =
        Records.newRecord(FederationPolicyConfigurationGetResponse.class);
    response.setPolicy(policy);
    return response;
  }

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