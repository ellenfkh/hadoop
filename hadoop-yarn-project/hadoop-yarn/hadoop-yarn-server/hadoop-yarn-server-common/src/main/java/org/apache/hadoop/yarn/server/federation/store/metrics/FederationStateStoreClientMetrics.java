/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.metrics;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(about = "Performance and usage metrics for Federation StateStore", context = "fedr")

/**
 * Performance metrics for FederationStateStore implementations
 */
public class FederationStateStoreClientMetrics implements MetricsSource {
  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreClientMetrics.class);

  private static final MetricsRegistry registry =
      new MetricsRegistry("FederationStateStoreClientMetrics");
  private final static Method[] StateStoreApiMethods =
      FederationStateStore.class.getMethods();

  // Map method names to counter objects
  private static final Map<String, MutableCounterLong> apiToFailedCalls =
      new HashMap<String, MutableCounterLong>();
  private static final Map<String, MutableRate> apiToSuccessfulCalls =
      new HashMap<String, MutableRate>();

  // Provide quantile latency for each api call.
  private static final Map<String, MutableQuantiles> apiToQuantileLatency =
      new HashMap<String, MutableQuantiles>();

  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Total number of successful calls and latency(ms)")
  private static MutableRate totalSucceededCalls;

  @Metric("Total number of failed StateStore calls")
  private static MutableCounterLong totalFailedCalls;

  // This after the static members are initialized, or the constructor will
  // throw a NullPointerException
  private static final FederationStateStoreClientMetrics s_instance =
      DefaultMetricsSystem.instance()
          .register(new FederationStateStoreClientMetrics());

  synchronized public static FederationStateStoreClientMetrics getInstance() {
    return s_instance;
  }

  private FederationStateStoreClientMetrics() {
    // Create the metrics for each method and put them into the map
    for (Method m : StateStoreApiMethods) {
      String methodName = m.getName();
      LOG.debug("Registering Federation StateStore Client metrics for {}",
          methodName);

      // This metric only records the number of failed calls; it does not
      // capture latency information
      apiToFailedCalls.put(methodName,
          registry.newCounter(methodName + "_numFailedCalls",
              "# failed calls to " + methodName, 0L));

      // This metric records both the number and average latency of successful
      // calls.
      apiToSuccessfulCalls.put(methodName,
          registry.newRate(methodName + "_successfulCalls",
              "# successful calls and latency(ms) for" + methodName));

      // This metric records the quantile-based latency of each successful call,
      // re-sampled every 10 seconds.
      apiToQuantileLatency.put(methodName, registry.newQuantiles(methodName
          + "Latency", "Quantile latency (ms) for " + methodName, "ops",
          "latency", 10));
    }
  }

  public static void failedStateStoreCall() {
    String methodName =
        Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableCounterLong methodMetric = apiToFailedCalls.get(methodName);
    if (methodMetric == null) {
      LOG.error(
          "Not recording failed call for unknown FederationStateStore method {}",
          methodName);
      return;
    }

    totalFailedCalls.incr();
    methodMetric.incr();
  }

  public static void succeededStateStoreCall(long duration) {
    String methodName =
        Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableRate methodMetric = apiToSuccessfulCalls.get(methodName);
    MutableQuantiles methodQuantileMetric = apiToQuantileLatency.get(methodName);
    if (methodMetric == null || methodQuantileMetric == null) {
      LOG.error(
          "Not recording successful call for unknown FederationStateStore method {}",
          methodName);
      return;
    }

    totalSucceededCalls.add(duration);
    methodMetric.add(duration);
    methodQuantileMetric.add(duration);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  // Getters for unit testing
  @VisibleForTesting
  static long getNumFailedCallsForMethod(String methodName) {
    return apiToFailedCalls.get(methodName).value();
  }

  @VisibleForTesting
  static long getNumSucceessfulCallsForMethod(String methodName) {
    return apiToSuccessfulCalls.get(methodName).lastStat().numSamples();
  }

  @VisibleForTesting
  static double getLatencySucceessfulCallsForMethod(String methodName) {
    return apiToSuccessfulCalls.get(methodName).lastStat().mean();
  }

  @VisibleForTesting
  static long getNumFailedCalls() {
    return totalFailedCalls.value();
  }

  @VisibleForTesting
  static long getNumSucceededCalls() {
    return totalSucceededCalls.lastStat().numSamples();
  }

  @VisibleForTesting
  static double getLatencySucceededCalls() {
    return totalSucceededCalls.lastStat().mean();
  }
}
