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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationApplicationStateVersionProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationApplicationStateVersionProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.federation.api.records.FederationApplicationStateVersion;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;

/**
 * Protocol buffer based implementation of
 * {@link FederationApplicationStateVersion}.
 */
@Private
@Unstable
public class FederationApplicationStateVersionPBImpl
    extends FederationApplicationStateVersion {
  private FederationApplicationStateVersionProto proto = null;
  private FederationApplicationStateVersionProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationApplicationStateVersionPBImpl() {
    builder = FederationApplicationStateVersionProto.newBuilder();
  }

  public FederationApplicationStateVersionPBImpl(
      FederationApplicationStateVersionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationApplicationStateVersionProto getProto() {
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
      builder = FederationApplicationStateVersionProto.newBuilder(proto);
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
  public Version getVersion() {
    FederationApplicationStateVersionProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasVersion()) {
      return null;
    }
    return convertFromProtoFormat(p.getVersion());
  }

  @Override
  public void setVersion(Version version) {
    maybeInitBuilder();
    if (version == null) {
      builder.clearVersion();
      return;
    }
    builder.setVersion(convertToProtoFormat(version));
  }

  private Version convertFromProtoFormat(VersionProto sc) {
    return new VersionPBImpl(sc);
  }

  private VersionProto convertToProtoFormat(Version scState) {
    return ((VersionPBImpl) scState).getProto();
  }

}