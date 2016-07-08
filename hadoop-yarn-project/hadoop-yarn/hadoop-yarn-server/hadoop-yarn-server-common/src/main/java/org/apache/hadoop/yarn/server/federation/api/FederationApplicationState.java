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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.api.records.*;

import java.util.Map;

/**
 * FederationApplicationState maintains the state of all <em>Applications</em>
 * that have been submitted to the federated cluster.
 *
 */
public interface FederationApplicationState {

  /**
   * Get the {@link FederationApplicationStateVersion} of the underlying
   * federation application state store.
   *
   * @return the {@link FederationApplicationStateVersion} of the underlying
   *         federation application state store
   */
  public FederationApplicationStateVersion getApplicationStateVersion();

  /**
   * Register the home {@link FederationSubClusterId} of the newly submitted
   * {@link ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param request the request to register a new application with its home
   *                sub-cluster
   * @return empty on successful registration of the application in the
   * StateStore, if not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  public FederationInsertNewApplicationResponse insertNewApplication(
      FederationInsertNewApplicationRequest request) throws YarnException;

  /**
   * Update the home {@link FederationSubClusterId} of a previously submitted
   * {@link ApplicationId}. Currently response is empty if the operation was
   * successful, if not an exception reporting reason for a failure.
   *
   * @param request the request to update the home
   *                sub-cluster of  an application.
   * @return empty on successful update of the application in the
   * StateStore, if not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  public FederationUpdateApplicationResponse updateApplication(
      FederationUpdateApplicationRequest request) throws YarnException;

  /**
   * Get information about the application identified by the input {@link
   * ApplicationId}.
   *
   * @param appId the application queried
   * @return {@link FederationApplicationInfo} containing the application's home
   *         subcluster
   * @throws YarnException if the request is invalid/fails
   */
  public FederationApplicationInfo getApplicationInfo(ApplicationId appId)
      throws YarnException;

  /**
   * Returns a map of information about all the application(s) keyed by the
   * {@link ApplicationId}.
   *
   * @return a map of {@link FederationApplicationInfo} keyed by the {@link
   * ApplicationId}
   * @throws YarnException if the request is invalid/fails
   */
  public Map<ApplicationId, FederationApplicationInfo> getAllApplicationsInfo()
      throws YarnException;

  /**
   * Delete the mapping of home {@link FederationSubClusterId} of a previously
   * submitted {@link ApplicationId}. Currently response is empty if the
   * operation was successful, if not an exception reporting reason for a
   * failure.
   *
   * @param request the request to delete the home sub-cluster of  an
   *                application.
   * @return empty on successful update of the application in the StateStore, if
   * not an exception reporting reason for a failure
   * @throws YarnException if the request is invalid/fails
   */
  public FederationDeleteApplicationResponse deleteApplication(
      FederationDeleteApplicationRequest request) throws YarnException;

}
