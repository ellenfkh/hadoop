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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.FederationSubClusterIdProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.api.records.FederationSubClusterId;

/**
 * Protocol buffer based implementation of {@link FederationSubClusterId}.
 */
@Private
@Unstable
public class FederationSubClusterIdPBImpl extends FederationSubClusterId {

  private FederationSubClusterIdProto proto =
      FederationSubClusterIdProto.getDefaultInstance();
  private FederationSubClusterIdProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationSubClusterIdPBImpl() {
    builder = FederationSubClusterIdProto.newBuilder();
  }

  public FederationSubClusterIdPBImpl(FederationSubClusterIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FederationSubClusterIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FederationSubClusterIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getId() {
    FederationSubClusterIdProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  protected void setId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearId();
      return;
    }
    builder.setId(subClusterId);
  }

}
