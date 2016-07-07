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

import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterInfoProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterState;

/**
 * Protocol buffer based implementation of {@link FederationSubClusterInfo}.
 */
@Private
@Unstable
public class FederationSubClusterInfoPBImpl extends FederationSubClusterInfo {

  private FederationSubClusterInfoProto proto =
      FederationSubClusterInfoProto.getDefaultInstance();
  private FederationSubClusterInfoProto.Builder builder = null;
  private boolean viaProto = false;

  private FederationSubClusterId subClusterId = null;

  public FederationSubClusterInfoPBImpl() {
    builder = FederationSubClusterInfoProto.newBuilder();
  }

  public FederationSubClusterInfoPBImpl(FederationSubClusterInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationSubClusterInfoProto getProto() {
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
      builder = FederationSubClusterInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.subClusterId != null) {
      builder.setSubClusterId(convertToProtoFormat(this.subClusterId));
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
      return super.equals(other);
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public FederationSubClusterId getSubClusterId() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (this.subClusterId != null) {
      return this.subClusterId;
    }
    if (!p.hasSubClusterId()) {
      return null;
    }
    this.subClusterId = convertFromProtoFormat(p.getSubClusterId());
    return this.subClusterId;
  }

  @Override
  public void setSubClusterId(FederationSubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
    }
    this.subClusterId = subClusterId;
  }

  @Override
  public String getAMRMAddress() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAMRMAddress()) ? p.getAMRMAddress() : null;
  }

  @Override
  public void setAMRMAddress(String amRMAddress) {
    maybeInitBuilder();
    if (amRMAddress == null) {
      builder.clearAMRMAddress();
      return;
    }
    builder.setAMRMAddress(amRMAddress);
  }

  @Override
  public String getClientRMAddress() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasClientRMAddress()) ? p.getClientRMAddress() : null;
  }

  @Override
  public void setClientRMAddress(String clientRMAddress) {
    maybeInitBuilder();
    if (clientRMAddress == null) {
      builder.clearClientRMAddress();
      return;
    }
    builder.setClientRMAddress(clientRMAddress);
  }

  @Override
  public String getRMAdminAddress() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRMAdminAddress()) ? p.getRMAdminAddress() : null;
  }

  @Override
  public void setRMAdminAddress(String rmAdminAddress) {
    maybeInitBuilder();
    if (rmAdminAddress == null) {
      builder.clearRMAdminAddress();
      return;
    }
    builder.setRMAdminAddress(rmAdminAddress);
  }

  @Override
  public String getWebAppAddress() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasWebAppAddress()) ? p.getWebAppAddress() : null;
  }

  @Override
  public void setWebAppAddress(String webAppAddress) {
    maybeInitBuilder();
    if (webAppAddress == null) {
      builder.clearWebAppAddress();
      return;
    }
    builder.setWebAppAddress(webAppAddress);
  }

  @Override
  public long getLastHeartBeat() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLastHeartBeat();
  }

  @Override
  public void setLastHeartBeat(long time) {
    maybeInitBuilder();
    builder.setLastHeartBeat(time);
  }

  @Override
  public FederationSubClusterState getState() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
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

  @Override
  public long getLastStartTime() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasLastStartTime()) ? p.getLastStartTime() : 0;
  }

  @Override
  public void setLastStartTime(long lastStartTime) {
    Preconditions.checkNotNull(builder);
    builder.setLastStartTime(lastStartTime);
  }

  @Override
  public String getCapability() {
    FederationSubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCapability()) ? p.getCapability() : null;
  }

  @Override
  public void setCapability(String capability) {
    maybeInitBuilder();
    if (capability == null) {
      builder.clearCapability();
      return;
    }
    builder.setCapability(capability);
  }

  private FederationSubClusterId convertFromProtoFormat(
      FederationSubClusterIdProto clusterId) {
    return new FederationSubClusterIdPBImpl(clusterId);
  }

  private FederationSubClusterIdProto convertToProtoFormat(
      FederationSubClusterId clusterId) {
    return ((FederationSubClusterIdPBImpl) clusterId).getProto();
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
