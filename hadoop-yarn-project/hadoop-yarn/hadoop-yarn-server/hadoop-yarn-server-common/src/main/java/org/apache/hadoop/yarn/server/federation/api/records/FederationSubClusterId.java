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
 * <p> FederationSubClusterId represents the <em>globally unique</em> identifier
 * for a subcluster that is participating in federation.
 *
 * <p> The globally unique nature of the identifier is obtained from the
 * <code>FederationMembershipState</code> on initialization.
 */
@Public
@Unstable
public abstract class FederationSubClusterId
    implements Comparable<FederationSubClusterId> {

  @Private
  @Unstable
  public static FederationSubClusterId newInstance(String subClusterId) {
    FederationSubClusterId id = Records.newRecord(FederationSubClusterId.class);
    id.setId(subClusterId);
    return id;
  }

  /**
   * Get the string identifier of the <em>subcluster</em> which is unique across
   * the federated cluster. The identifier is static, i.e. preserved across
   * restarts and failover.
   *
   * @return unique identifier of the subcluster
   */
  @Public
  @Unstable
  public abstract String getId();

  /**
   * Set the string identifier of the <em>subcluster</em> which is unique across
   * the federated cluster. The identifier is static, i.e. preserved across
   * restarts and failover.
   *
   * @param subClusterId unique identifier of the subcluster
   */
  @Private
  @Unstable
  protected abstract void setId(String subClusterId);

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
    FederationSubClusterId other = (FederationSubClusterId) obj;
    return this.getId().equals(other.getId());
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  @Override
  public int compareTo(FederationSubClusterId other) {
    return getId().compareTo(other.getId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getId());
    return sb.toString();
  }

}
