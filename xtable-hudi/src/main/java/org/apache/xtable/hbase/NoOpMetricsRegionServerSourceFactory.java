/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.hbase;

import org.apache.hadoop.hbase.io.MetricsIOSource;
import org.apache.hadoop.hbase.io.MetricsIOWrapper;
import org.apache.hadoop.hbase.regionserver.MetricsHeapMemoryManagerSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper;
import org.apache.hadoop.hbase.regionserver.MetricsRegionSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionWrapper;
import org.apache.hadoop.hbase.regionserver.MetricsTableAggregateSource;
import org.apache.hadoop.hbase.regionserver.MetricsTableSource;
import org.apache.hadoop.hbase.regionserver.MetricsTableWrapperAggregate;
import org.apache.hadoop.hbase.regionserver.MetricsUserAggregateSource;
import org.apache.hadoop.hbase.regionserver.MetricsUserSource;

/** Provides a No-Op metrics implementation for the HFile required by Hudi. */
public class NoOpMetricsRegionServerSourceFactory implements MetricsRegionServerSourceFactory {
  @Override
  public MetricsRegionServerSource createServer(MetricsRegionServerWrapper regionServerWrapper) {
    return null;
  }

  @Override
  public MetricsRegionSource createRegion(MetricsRegionWrapper wrapper) {
    return null;
  }

  @Override
  public MetricsUserSource createUser(String shortUserName) {
    return null;
  }

  @Override
  public MetricsUserAggregateSource getUserAggregate() {
    return null;
  }

  @Override
  public MetricsTableSource createTable(String table, MetricsTableWrapperAggregate wrapper) {
    return null;
  }

  @Override
  public MetricsTableAggregateSource getTableAggregate() {
    return null;
  }

  @Override
  public MetricsHeapMemoryManagerSource getHeapMemoryManager() {
    return null;
  }

  @Override
  public MetricsIOSource createIO(MetricsIOWrapper wrapper) {
    return new NoOpMetricsIOSource();
  }

  private static class NoOpMetricsIOSource implements MetricsIOSource {

    @Override
    public void updateFsReadTime(long t) {}

    @Override
    public void updateFsPReadTime(long t) {}

    @Override
    public void updateFsWriteTime(long t) {}

    @Override
    public void init() {}

    @Override
    public void setGauge(String gaugeName, long value) {}

    @Override
    public void incGauge(String gaugeName, long delta) {}

    @Override
    public void decGauge(String gaugeName, long delta) {}

    @Override
    public void removeMetric(String key) {}

    @Override
    public void incCounters(String counterName, long delta) {}

    @Override
    public void updateHistogram(String name, long value) {}

    @Override
    public String getMetricsContext() {
      return "";
    }

    @Override
    public String getMetricsDescription() {
      return "";
    }

    @Override
    public String getMetricsJmxContext() {
      return "";
    }

    @Override
    public String getMetricsName() {
      return "";
    }
  }
}
