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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationApplicationInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationUpdateApplicationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationUpdateApplicationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.api.records.FederationApplicationInfo;
import org.apache.hadoop.yarn.server.federation.api.records.FederationUpdateApplicationRequest;

/**
 * Protocol buffer based implementation of
 * {@link FederationUpdateApplicationRequest}.
 */
@Private
@Unstable
public class FederationUpdateApplicationRequestPBImpl
    extends FederationUpdateApplicationRequest {

  private FederationUpdateApplicationRequestProto proto =
      FederationUpdateApplicationRequestProto.getDefaultInstance();
  private FederationUpdateApplicationRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationUpdateApplicationRequestPBImpl() {
    builder = FederationUpdateApplicationRequestProto.newBuilder();
  }

  public FederationUpdateApplicationRequestPBImpl(
      FederationUpdateApplicationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationUpdateApplicationRequestProto getProto() {
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
      builder = FederationUpdateApplicationRequestProto.newBuilder(proto);
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
  public FederationApplicationInfo getApplicationInfo() {
    FederationUpdateApplicationRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasApplicationInfo()) {
      return null;
    }
    return convertFromProtoFormat(p.getApplicationInfo());
  }

  @Override
  public void setApplicationInfo(FederationApplicationInfo applicationInfo) {
    maybeInitBuilder();
    if (applicationInfo == null) {
      builder.clearApplicationInfo();
      return;
    }
    builder.setApplicationInfo(convertToProtoFormat(applicationInfo));
  }

  private FederationApplicationInfo convertFromProtoFormat(
      FederationApplicationInfoProto sc) {
    return new FederationApplicationInfoPBImpl(sc);
  }

  private FederationApplicationInfoProto convertToProtoFormat(
      FederationApplicationInfo sc) {
    return ((FederationApplicationInfoPBImpl) sc).getProto();
  }

}
