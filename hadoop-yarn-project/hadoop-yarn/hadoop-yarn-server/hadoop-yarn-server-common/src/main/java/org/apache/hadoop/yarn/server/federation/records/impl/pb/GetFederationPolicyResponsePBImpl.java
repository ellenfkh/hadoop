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

package org.apache.hadoop.yarn.server.federation.records.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationPolicyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetFederationPolicyResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetFederationPolicyResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.records.FederationPolicy;
import org.apache.hadoop.yarn.server.federation.records.GetFederationPolicyResponse;

/**
 * Protocol buffer based implementation of {@link GetFederationPolicyResponse}.
 */
@Private
@Unstable
public class GetFederationPolicyResponsePBImpl
    extends GetFederationPolicyResponse {

  private GetFederationPolicyResponseProto proto =
      GetFederationPolicyResponseProto.getDefaultInstance();
  private GetFederationPolicyResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private FederationPolicy federationPolicy = null;

  public GetFederationPolicyResponsePBImpl() {
    builder = GetFederationPolicyResponseProto.newBuilder();
  }

  public GetFederationPolicyResponsePBImpl(
      GetFederationPolicyResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetFederationPolicyResponseProto getProto() {
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
      builder = GetFederationPolicyResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.federationPolicy != null
        && !((FederationPolicyPBImpl) this.federationPolicy).getProto()
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
  public FederationPolicy getPolicy() {
    GetFederationPolicyResponseProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setPolicy(FederationPolicy policy) {
    maybeInitBuilder();
    if (policy == null) {
      builder.clearPolicy();
    }
    this.federationPolicy = policy;
  }

  private FederationPolicy convertFromProtoFormat(
      FederationPolicyProto policy) {
    return new FederationPolicyPBImpl(policy);
  }

  private FederationPolicyProto convertToProtoFormat(FederationPolicy policy) {
    return ((FederationPolicyPBImpl) policy).getProto();
  }

}
