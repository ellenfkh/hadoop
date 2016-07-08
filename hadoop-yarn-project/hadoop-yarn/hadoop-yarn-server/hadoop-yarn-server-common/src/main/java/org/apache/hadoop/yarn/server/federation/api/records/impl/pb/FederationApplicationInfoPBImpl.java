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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationApplicationInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationApplicationInfoProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.server.federation.api.records.FederationApplicationInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;

/**
 * Protocol buffer based implementation of {@link FederationApplicationInfo}.
 */
@Private
@Unstable
public class FederationApplicationInfoPBImpl extends FederationApplicationInfo {

  private FederationApplicationInfoProto proto =
      FederationApplicationInfoProto.getDefaultInstance();
  private FederationApplicationInfoProto.Builder builder = null;
  private boolean viaProto = false;

  private ApplicationId applicationId = null;
  private FederationSubClusterId homeSubCluster = null;

  public FederationApplicationInfoPBImpl() {
    builder = FederationApplicationInfoProto.newBuilder();
  }

  public FederationApplicationInfoPBImpl(FederationApplicationInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationApplicationInfoProto getProto() {
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
      builder = FederationApplicationInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.homeSubCluster != null) {
      builder.setHomeSubCluster(convertToProtoFormat(this.homeSubCluster));
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
  public ApplicationId getApplicationId() {
    FederationApplicationInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId  = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
      return;
    }
    this.applicationId = applicationId;
  }

  @Override
  public FederationSubClusterId getHomeSubCluster() {
    FederationApplicationInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (this.homeSubCluster != null) {
      return this.homeSubCluster;
    }
    if (!p.hasHomeSubCluster()) {
      return null;
    }
    this.homeSubCluster = convertFromProtoFormat(p.getHomeSubCluster());
    return this.homeSubCluster;
  }

  @Override
  public void setHomeSubCluster(FederationSubClusterId homeSubCluster) {
    maybeInitBuilder();
    if (homeSubCluster == null) {
      builder.clearHomeSubCluster();
    }
    this.homeSubCluster = homeSubCluster;
  }

  private FederationSubClusterId convertFromProtoFormat(
      FederationSubClusterIdProto subClusterId) {
    return new FederationSubClusterIdPBImpl(subClusterId);
  }

  private FederationSubClusterIdProto convertToProtoFormat(
      FederationSubClusterId subClusterId) {
    return ((FederationSubClusterIdPBImpl) subClusterId).getProto();
  }

  private ApplicationId convertFromProtoFormat(ApplicationIdProto appId) {
    return new ApplicationIdPBImpl(appId);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId appId) {
    return ((ApplicationIdPBImpl) appId).getProto();
  }
}
