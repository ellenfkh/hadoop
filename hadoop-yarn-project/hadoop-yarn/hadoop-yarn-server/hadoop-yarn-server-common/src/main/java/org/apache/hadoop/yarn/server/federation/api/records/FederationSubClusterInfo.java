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
 * FederationSubClusterInfo is a report of the runtime information of the
 * subcluster that is participating in federation.
 *
 * <p>
 * It includes information such as:
 * <ul>
 * <li>{@link FederationSubClusterId}</li>
 * <li>The URL of the subcluster</li>
 * <li>The timestamp representing the last start time of the subCluster</li>
 * <li>{@code FederationsubClusterState}</li>
 * <li>The current capacity and utilization of the subCluster</li>
 * </ul>
 */
@Public
@Unstable
public abstract class FederationSubClusterInfo {

  @Private
  @Unstable
  public static FederationSubClusterInfo newInstance(
      FederationSubClusterId subClusterId, String amRMAddress,
      String clientRMAddress, String rmAdminAddress, String webAppAddress,
      FederationSubClusterState state, long lastStartTime, String capability) {
    return newInstance(subClusterId, amRMAddress, clientRMAddress,
        rmAdminAddress, webAppAddress, 0, state, lastStartTime, capability);
  }

  @Private
  @Unstable
  public static FederationSubClusterInfo newInstance(
      FederationSubClusterId subClusterId, String amRMAddress,
      String clientRMAddress, String rmAdminAddress, String webAppAddress,
      long lastHeartBeat, FederationSubClusterState state, long lastStartTime,
      String capability) {
    FederationSubClusterInfo subClusterInfo =
        Records.newRecord(FederationSubClusterInfo.class);
    subClusterInfo.setSubClusterId(subClusterId);
    subClusterInfo.setAMRMAddress(amRMAddress);
    subClusterInfo.setClientRMAddress(clientRMAddress);
    subClusterInfo.setRMAdminAddress(rmAdminAddress);
    subClusterInfo.setWebAppAddress(webAppAddress);
    subClusterInfo.setLastHeartBeat(lastHeartBeat);
    subClusterInfo.setState(state);
    subClusterInfo.setLastStartTime(lastStartTime);
    subClusterInfo.setCapability(capability);
    return subClusterInfo;
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
   * of the subCluster.
   *
   * @param subClusterId the subCluster identifier
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(FederationSubClusterId subClusterId);

  /**
   * Get the URL of the AM-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @return the URL of the AM-RM service endpoint of the subcluster
   *         <code>ResourceManager</code>
   */
  @Public
  @Unstable
  public abstract String getAMRMAddress();

  /**
   * Set the URL of the AM-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @param amRMAddress the URL of the AM-RM service endpoint of the subcluster
   *          <code>ResourceManager</code>
   */
  @Private
  @Unstable
  public abstract void setAMRMAddress(String amRMAddress);

  /**
   * Get the URL of the client-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @return the URL of the client-RM service endpoint of the subcluster
   *         <code>ResourceManager</code>
   */
  @Public
  @Unstable
  public abstract String getClientRMAddress();

  /**
   * Set the URL of the client-RM service endpoint of the subcluster
   * <code>ResourceManager</code>.
   *
   * @param clientRMAddress the URL of the client-RM service endpoint of the
   *          subCluster <code>ResourceManager</code>
   */
  @Private
  @Unstable
  public abstract void setClientRMAddress(String clientRMAddress);

  /**
   * Get the URL of the <code>ResourceManager</code> administration service.
   *
   * @return the URL of the <code>ResourceManager</code> administration service
   */
  @Public
  @Unstable
  public abstract String getRMAdminAddress();

  /**
   * Set the URL of the <code>ResourceManager</code> administration service.
   *
   * @param rmAdminAddress the URL of the <code>ResourceManager</code>
   *          administration service.
   */
  @Private
  @Unstable
  public abstract void setRMAdminAddress(String rmAdminAddress);

  /**
   * Get the URL of the <code>ResourceManager</code> web application interface.
   *
   * @return the URL of the <code>ResourceManager</code> web application
   *         interface.
   */
  @Public
  @Unstable
  public abstract String getWebAppAddress();

  /**
   * Set the URL of the <code>ResourceManager</code> web application interface.
   *
   * @param webAppAddress the URL of the <code>ResourceManager</code> web
   *          application interface.
   */
  @Private
  @Unstable
  public abstract void setWebAppAddress(String webAppAddress);

  /**
   * Get the last heart beat time of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract long getLastHeartBeat();

  /**
   * Set the last heartbeat time of the subcluster.
   *
   * @param time the last heartbeat time of the subcluster
   */
  @Private
  @Unstable
  public abstract void setLastHeartBeat(long time);

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

  /**
   * Get the timestamp representing the last start time of the subcluster.
   *
   * @return the timestamp representing the last start time of the subcluster
   */
  @Public
  @Unstable
  public abstract long getLastStartTime();

  /**
   * Set the timestamp representing the last start time of the subcluster.
   *
   * @param lastStartTime the timestamp representing the last start time of the
   *          subcluster
   */
  @Private
  @Unstable
  public abstract void setLastStartTime(long lastStartTime);

  /**
   * Get the current capacity and utilization of the subcluster. This is the
   * JAXB marshalled string representation of the <code>ClusterMetrics</code>.
   *
   * @return the current capacity and utilization of the subcluster
   */
  @Public
  @Unstable
  public abstract String getCapability();

  /**
   * Set the current capacity and utilization of the subCluster. This is the
   * JAXB marshalled string representation of the <code>ClusterMetrics</code>.
   *
   * @param capability the current capacity and utilization of the subcluster
   */
  @Private
  @Unstable
  public abstract void setCapability(String capability);

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FederationSubClusterInfo other = (FederationSubClusterInfo) obj;
    if (!this.getSubClusterId().equals(other.getSubClusterId())) {
      return false;
    }
    if (!this.getAMRMAddress().equals(other.getAMRMAddress())) {
      return false;
    }
    if (!this.getClientRMAddress().equals(other.getClientRMAddress())) {
      return false;
    }
    if (!this.getRMAdminAddress().equals(other.getRMAdminAddress())) {
      return false;
    }
    if (!this.getWebAppAddress().equals(other.getWebAppAddress())) {
      return false;
    }
    if (!this.getState().equals(other.getState())) {
      return false;
    }
    return this.getLastStartTime() == other.getLastStartTime();
    //Capability is not necessary to compare the information
  }

  @Override
  public int hashCode() {
    return getSubClusterId().hashCode();
  }

  @Override
  public String toString() {
    return "FederationSubClusterInfo [getSubClusterId() = " + getSubClusterId()
        + ", getAMRMAddress() = " + getAMRMAddress()
        + ", getClientRMAddress() = " + getClientRMAddress()
        + ", getRMAdminAddress() = " + getRMAdminAddress()
        + ", getWebAppAddress() = " + getWebAppAddress() + ", getState() = "
        + getState() + ", getLastStartTime() = " + getLastStartTime()
        + ", getCapability() = " + getCapability() + "]";
  }
}
