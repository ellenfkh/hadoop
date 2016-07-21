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

package org.apache.hadoop.yarn.server.federation.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.api.FederationApplicationState;
import org.apache.hadoop.yarn.server.federation.api.records.FederationApplicationInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationApplicationStateVersion;
import org.apache.hadoop.yarn.server.federation.api.records.FederationDeleteApplicationRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationDeleteApplicationResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationInsertNewApplicationRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationInsertNewApplicationResponse;
import org.apache.hadoop.yarn.server.federation.api.records.FederationUpdateApplicationRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationUpdateApplicationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class FederationInMemoryApplicationState
    implements FederationApplicationState {
  static final Logger LOG =
      LoggerFactory.getLogger(FederationInMemoryApplicationState.class);

  private final Map<ApplicationId, FederationApplicationInfo> applications =
      new ConcurrentHashMap<ApplicationId, FederationApplicationInfo>();

  public FederationInMemoryApplicationState() {
  }

  @Override
  public FederationApplicationStateVersion getApplicationStateVersion() {
    return null;
  }

  @Override
  public FederationInsertNewApplicationResponse insertNewApplication(
      FederationInsertNewApplicationRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationInfo().getApplicationId();

    if (applications.containsKey(appId)) {
      throw new YarnException(
          "Application " + appId.toString() + " already exists, cannot insert");
    }

    applications.put(appId, request.getApplicationInfo());
    return FederationInsertNewApplicationResponse.newInstance();
  }

  @Override
  public FederationUpdateApplicationResponse updateApplication(
      FederationUpdateApplicationRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationInfo().getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException(
          "Application " + appId.toString() + " does not exist, cannot update");
    }

    applications.put(appId, request.getApplicationInfo());
    return FederationUpdateApplicationResponse.newInstance();
  }

  @Override
  public FederationApplicationInfo getApplicationInfo(ApplicationId appId)
      throws YarnException {
    return applications.get(appId);
  }

  @Override
  public Map<ApplicationId, FederationApplicationInfo> getAllApplicationsInfo()
      throws YarnException {
    return applications;
  }

  @Override
  public FederationDeleteApplicationResponse deleteApplication(
      FederationDeleteApplicationRequest request) throws YarnException {
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      throw new YarnException(
          "Application " + appId + " does not exist to delete");
    }

    applications.remove(appId);
    return FederationDeleteApplicationResponse.newInstance();

  }

  @VisibleForTesting
  public Map<ApplicationId, FederationApplicationInfo> getApplicationsTable() {
    return applications;
  }

  @VisibleForTesting
  public void clearApplicationsTable() {
    applications.clear();
  }
}
