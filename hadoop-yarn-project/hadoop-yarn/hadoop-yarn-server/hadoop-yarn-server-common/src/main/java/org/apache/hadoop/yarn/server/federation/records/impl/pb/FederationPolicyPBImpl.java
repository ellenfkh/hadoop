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
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationPolicyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationPolicyProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.records.FederationPolicy;

import java.nio.ByteBuffer;

/**
 * Protocol buffer based implementation of {@link FederationPolicy}.
 */
@Private
@Unstable
public class FederationPolicyPBImpl extends FederationPolicy {

  private FederationPolicyProto proto =
      FederationPolicyProto.getDefaultInstance();
  private FederationPolicyProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationPolicyPBImpl() {
    builder = FederationPolicyProto.newBuilder();
  }

  public FederationPolicyPBImpl(FederationPolicyProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationPolicyProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FederationPolicyProto.newBuilder(proto);
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
  public String getType() {
    FederationPolicyProtoOrBuilder p = viaProto ? proto : builder;
    return p.getType();
  }

  @Override
  public void setType(String policyType) {
    maybeInitBuilder();
    if (policyType == null) {
      builder.clearType();
      return;
    }
    builder.setType(policyType);
  }

  @Override
  public ByteBuffer getParams() {
    FederationPolicyProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getParams());
  }

  @Override
  public void setParams(ByteBuffer policyParams) {
    maybeInitBuilder();
    if (policyParams == null) {
      builder.clearParams();
      return;
    }
    builder.setParams(ProtoUtils.convertToProtoFormat(policyParams));
  }

}