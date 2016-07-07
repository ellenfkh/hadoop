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
 * <p>
 * The request sent to set the state of a subcluster to either
 * SC_DECOMMISSIONED, SC_LOST, or SC_DEREGISTERED.
 *
 * <p>
 * The update includes details such as:
 * <ul>
 * <li>{@link FederationSubClusterId}</li>
 * <li>{@link FederationSubClusterState}</li>
 * </ul>
 */
@Public
@Unstable
public abstract class FederationSubClusterDeregisterRequest {

  @Private
  @Unstable
  public static FederationSubClusterDeregisterRequest newInstance(
      FederationSubClusterId subClusterId,
      FederationSubClusterState subClusterState) {
    FederationSubClusterDeregisterRequest registerRequest =
        Records.newRecord(FederationSubClusterDeregisterRequest.class);
    registerRequest.setSubClusterId(subClusterId);
    registerRequest.setState(subClusterState);
    return registerRequest;
  }

  /**
   * Get the {@link FederationSubClusterId} representing the unique identifier
   * of the subcluster.
   *
   * @return the subcluster identifier
   */
  @Public
  @Unstable
  public abstract FederationSubClusterId getSubClusterId();

  /**
   * Set the {@link FederationSubClusterId} representing the unique identifier
   * of the subcluster.
   *
   * @param subClusterId the subcluster identifier
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(FederationSubClusterId subClusterId);

  /**
   * Get the {@link FederationSubClusterState} of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract FederationSubClusterState getState();

  /**
   * Set the {@link FederationSubClusterState} of the subcluster.
   *
   * @param state the state of the subCluster
   */
  @Private
  @Unstable
  public abstract void setState(FederationSubClusterState state);
}
