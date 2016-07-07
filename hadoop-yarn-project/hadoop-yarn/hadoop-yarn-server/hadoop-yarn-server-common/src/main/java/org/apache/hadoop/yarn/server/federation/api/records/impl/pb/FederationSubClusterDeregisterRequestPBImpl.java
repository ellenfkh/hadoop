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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterDeregisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterDeregisterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterState;

/**
 * Protocol buffer based implementation of
 * {@link FederationSubClusterDeregisterRequest}.
 */
@Private
@Unstable
public class FederationSubClusterDeregisterRequestPBImpl
    extends FederationSubClusterDeregisterRequest {

  private FederationSubClusterDeregisterRequestProto proto =
      FederationSubClusterDeregisterRequestProto.getDefaultInstance();
  private FederationSubClusterDeregisterRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationSubClusterDeregisterRequestPBImpl() {
    builder = FederationSubClusterDeregisterRequestProto.newBuilder();
  }

  public FederationSubClusterDeregisterRequestPBImpl(
      FederationSubClusterDeregisterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationSubClusterDeregisterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FederationSubClusterDeregisterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
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
  public FederationSubClusterId getSubClusterId() {
    FederationSubClusterDeregisterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasSubClusterId()) {
      return null;
    }
    return convertFromProtoFormat(p.getSubClusterId());
  }

  @Override
  public void setSubClusterId(FederationSubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  @Override
  public FederationSubClusterState getState() {
    FederationSubClusterDeregisterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setState(FederationSubClusterState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }

  private FederationSubClusterId convertFromProtoFormat(
      FederationSubClusterIdProto sc) {
    return new FederationSubClusterIdPBImpl(sc);
  }

  private FederationSubClusterIdProto convertToProtoFormat(
      FederationSubClusterId sc) {
    return ((FederationSubClusterIdPBImpl) sc).getProto();
  }

  private FederationSubClusterState convertFromProtoFormat(
      FederationSubClusterStateProto state) {
    return FederationSubClusterState.valueOf(state.name());
  }

  private FederationSubClusterStateProto convertToProtoFormat(
      FederationSubClusterState state) {
    return FederationSubClusterStateProto.valueOf(state.name());
  }

}
