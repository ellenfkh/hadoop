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

package org.apache.hadoop.yarn.server.federation.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * The FederationState encapsulates all the state that is required to federate
 * multiple YARN subclusters. This includes the membership information of
 * subclusters that are participating in federation as defined by {@link
 * FederationMembershipState} and information of all active applications in the
 * federated cluster as represented by {@link FederationApplicationState}.
 */
public interface FederationStateStore
    extends FederationMembershipState, FederationApplicationState {

  /**
   * Perform any initialization operation(s) of the StateStore.
   *
   * @param conf the cluster configuration
   * @throws YarnException if initialization fails
   */
  public void init(Configuration conf) throws YarnException;

  /**
   * Perform any cleanup operation(s) of the StateStore.
   *
   * @throws Exception if cleanup fails
   */
  public void close() throws Exception;
}
