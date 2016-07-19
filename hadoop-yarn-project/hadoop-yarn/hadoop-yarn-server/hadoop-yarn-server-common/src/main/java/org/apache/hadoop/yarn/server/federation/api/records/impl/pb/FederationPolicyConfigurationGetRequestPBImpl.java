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

package org.apache.hadoop.yarn.server.federation.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationPolicyConfigurationGetRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationPolicyConfigurationGetRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfigurationGetRequest;

import com.google.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of
 * {@link FederationPolicyConfigurationGetRequest}.
 */
@Private
@Unstable
public class FederationPolicyConfigurationGetRequestPBImpl
    extends FederationPolicyConfigurationGetRequest {

  private FederationPolicyConfigurationGetRequestProto proto =
      FederationPolicyConfigurationGetRequestProto.getDefaultInstance();
  private FederationPolicyConfigurationGetRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationPolicyConfigurationGetRequestPBImpl() {
    builder = FederationPolicyConfigurationGetRequestProto.newBuilder();
  }

  public FederationPolicyConfigurationGetRequestPBImpl(
      FederationPolicyConfigurationGetRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationPolicyConfigurationGetRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FederationPolicyConfigurationGetRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public String getQueue() {
    FederationPolicyConfigurationGetRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getQueue();
  }

  @Override
  public void setQueue(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queueName);
  }

}
