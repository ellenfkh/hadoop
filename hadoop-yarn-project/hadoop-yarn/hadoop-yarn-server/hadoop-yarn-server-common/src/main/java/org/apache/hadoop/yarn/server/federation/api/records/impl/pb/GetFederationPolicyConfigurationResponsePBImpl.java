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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationPolicyConfigurationProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetFederationPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetFederationPolicyConfigurationResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.api.records.FederationPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.api.records.GetFederationPolicyConfigurationResponse;

/**
 * Protocol buffer based implementation of {@link GetFederationPolicyConfigurationResponse}.
 */
@Private
@Unstable
public class GetFederationPolicyConfigurationResponsePBImpl
    extends GetFederationPolicyConfigurationResponse {

  private GetFederationPolicyConfigurationResponseProto proto =
      GetFederationPolicyConfigurationResponseProto.getDefaultInstance();
  private GetFederationPolicyConfigurationResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private FederationPolicyConfiguration federationPolicy = null;

  public GetFederationPolicyConfigurationResponsePBImpl() {
    builder = GetFederationPolicyConfigurationResponseProto.newBuilder();
  }

  public GetFederationPolicyConfigurationResponsePBImpl(
      GetFederationPolicyConfigurationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetFederationPolicyConfigurationResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetFederationPolicyConfigurationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.federationPolicy != null
        && !((FederationPolicyConfigurationPBImpl) this.federationPolicy).getProto()
        .equals(builder.getPolicy())) {
      builder.setPolicy(convertToProtoFormat(this.federationPolicy));
    }
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
  public FederationPolicyConfiguration getPolicy() {
    GetFederationPolicyConfigurationResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.federationPolicy != null) {
      return this.federationPolicy;
    }
    if (!p.hasPolicy()) {
      return null;
    }
    this.federationPolicy = convertFromProtoFormat(p.getPolicy());
    return this.federationPolicy;
  }

  @Override
  public void setPolicy(FederationPolicyConfiguration policy) {
    maybeInitBuilder();
    if (policy == null) {
      builder.clearPolicy();
    }
    this.federationPolicy = policy;
  }

  private FederationPolicyConfiguration convertFromProtoFormat(
      FederationPolicyConfigurationProto policy) {
    return new FederationPolicyConfigurationPBImpl(policy);
  }

  private FederationPolicyConfigurationProto convertToProtoFormat(FederationPolicyConfiguration policy) {
    return ((FederationPolicyConfigurationPBImpl) policy).getProto();
  }

}
