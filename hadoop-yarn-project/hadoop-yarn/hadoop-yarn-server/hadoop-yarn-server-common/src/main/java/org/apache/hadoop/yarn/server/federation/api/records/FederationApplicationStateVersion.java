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

package org.apache.hadoop.yarn.server.federation.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.util.Records;

/**
 * The {@link Version} information for the federation application state which
 * include: majorVersion and minorVersion. The major version update means
 * incompatible changes happen while minor version update indicates compatible
 * changes.
 */
@Public
@Unstable
public abstract class FederationApplicationStateVersion {

  @Private
  @Unstable
  public static FederationApplicationStateVersion newInstance() {
    FederationApplicationStateVersion response =
        Records.newRecord(FederationApplicationStateVersion.class);
    return response;
  }

  /**
   * Get the {@link Version} of the underlying federation application state
   * store.
   *
   * @return the {@link Version} of the underlying federation application state
   *         store
   */
  @Public
  @Unstable
  public abstract Version getVersion();

  /**
   * Set the {@link Version} of the underlying federation application state
   * store.
   *
   * @param version the {@link Version} of the underlying federation application
   *          state store
   */
  @Private
  @Unstable
  public abstract void setVersion(Version version);

}