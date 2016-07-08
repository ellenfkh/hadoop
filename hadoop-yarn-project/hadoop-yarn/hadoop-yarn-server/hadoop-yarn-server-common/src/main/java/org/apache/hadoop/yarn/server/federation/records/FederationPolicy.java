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

package org.apache.hadoop.yarn.server.federation.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;

/**
 * FederationPolicy is a representation of the user resources that have been
 * reserved in a sub-cluster. Currently it is encapsulated as a type name and a
 * byte array.
 */
@Public
@Unstable
public abstract class FederationPolicy {

  @Private
  @Unstable
  public static FederationPolicy newInstance(String policyName,
      byte[] policyDescriptor) {
    FederationPolicy policy = Records.newRecord(FederationPolicy.class);
    return policy;
  }

  /**
   * Get the type of the policy. This could be random, round-robin, load-based,
   * etc.
   *
   * @return the type of the policy
   */
  @Public
  @Unstable
  public abstract String getType();

  /**
   * Sets the type of the policy. This could be random, round-robin, load-based,
   * etc.
   *
   * @param policyType the type of the policy
   */
  @Private
  @Unstable
  public abstract void setType(String policyType);

  /**
   * Get the policy parameters. This affects how the policy behaves and an
   * example could be weight distribution of queues across multiple
   * sub-clusters.
   *
   * @return the byte array that contains the parameters
   */
  @Public
  @Unstable
  public abstract ByteBuffer getParams();

  /**
   * Set the policy parameters. This affects how the policy behaves and an
   * example could be weight distribution of queues across multiple
   * sub-clusters.
   *
   * @param policyParams byte array that describes the policy
   */
  @Private
  @Unstable
  public abstract void setParams(ByteBuffer policyParams);

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
    FederationPolicy other = (FederationPolicy) obj;
    if (this.getType() != other.getType()) {
      return false;
    }
    if (this.getParams() != other.getParams()) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType());
    sb.append(" : ");
    sb.append(getParams());
    return sb.toString();
  }
}