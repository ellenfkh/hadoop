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
import org.apache.hadoop.yarn.server.federation.api.records.FederationDeleteApplicationRequest;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationDeleteApplicationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationDeleteApplicationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

/**
 * Protocol buffer based implementation of
 * {@link FederationDeleteApplicationRequest}.
 */
@Private
@Unstable
public class FederationDeleteApplicationRequestPBImpl
    extends FederationDeleteApplicationRequest {

  private FederationDeleteApplicationRequestProto proto =
      FederationDeleteApplicationRequestProto.getDefaultInstance();
  private FederationDeleteApplicationRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationDeleteApplicationRequestPBImpl() {
    builder = FederationDeleteApplicationRequestProto.newBuilder();
  }

  public FederationDeleteApplicationRequestPBImpl(
      FederationDeleteApplicationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationDeleteApplicationRequestProto getProto() {
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
      builder = FederationDeleteApplicationRequestProto.newBuilder(proto);
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
  public ApplicationId getApplicationId() {
    FederationDeleteApplicationRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    return convertFromProtoFormat(p.getApplicationId());
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
      return;
    }
    builder.setApplicationId(convertToProtoFormat(applicationId));
  }

  private ApplicationId convertFromProtoFormat(
      ApplicationIdProto appId) {
    return new ApplicationIdPBImpl(appId);
  }

  private ApplicationIdProto convertToProtoFormat(
      ApplicationId appId) {
    return ((ApplicationIdPBImpl) appId).getProto();
  }

}
