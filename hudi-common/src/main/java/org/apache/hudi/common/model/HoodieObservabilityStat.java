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

package org.apache.hudi.common.model;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Observability related metrics collection operations.
 */
public class HoodieObservabilityStat implements Serializable {
  public static final String OBSERVABILITY_REGISTRY_NAME = "Observability";

  // define a unique metric name string for each metric to be collected.
  public static final String PARQUET_NORMALIZED_WRITE_TIME = "writeTimePerRecordInUSec";
  public static final String PARQUET_CUMULATIVE_WRITE_TIME = "cumulativeParquetWriteTimeInUSec";
  public static final String PARQUET_WRITE_TIME_PER_MB_IN_USEC = "writeTimePerMBInUSec";
  public static final String PARQUET_WRITE_THROUGHPUT_MBPS = "writeThroughputMBps";
  public static final String TOTAL_RECORDS_WRITTEN = "totalRecordsWritten";

  public enum WriteType {
    INSERT,
    UPSERT,
    UPDATE,
  }

  public static long ONE_MB = 1024 * 1024;
  public static long USEC_PER_SEC = 1000 * 1000;
  Option<Registry> observabilityRegistry;
  String tableName;
  WriteType writeType;
  String hostName;
  Long stageId;
  Long partitionId;

  public HoodieObservabilityStat(Option<Registry> registry, String tableName, WriteType type, String host,
                                 long stageId, long partitionId) {
    this.observabilityRegistry = registry;
    this.tableName = tableName;
    this.writeType = type;
    this.hostName = host;
    this.stageId = stageId;
    this.partitionId = partitionId;
  }

  private String getWriteMetricWithTypeHostNameAndPartitionId(String tableName, String metric, String type,
                                                              String host, long partitionId) {
    return String.format("%s.%s.%s.%s.%d", tableName, metric, type, host, partitionId);
  }

  private String getConsolidatedWriteMetricKey(String tableName, String metric, String type) {
    return String.format("%s.consolidated.%s.%s", tableName, metric, type);
  }

  public void recordWriteStats(long totalRecs, long cumulativeWriteTimeInMsec, long fileSizeInBytes) {

    long cumulativeWriteTimeInUsec = cumulativeWriteTimeInMsec * 1000;
    long writeTimePerRecordInUSec = cumulativeWriteTimeInUsec / Math.max(totalRecs, 1);
    long writeTimePerMBInUSec =
        (long)(cumulativeWriteTimeInUsec * ((double)ONE_MB / (double)Math.max(fileSizeInBytes, 1)));
    long writeThroughputMBps = (USEC_PER_SEC / Math.max(writeTimePerMBInUSec, 1));

    if (observabilityRegistry.isPresent()) {
      // executor specific stats
      observabilityRegistry.get().add(getWriteMetricWithTypeHostNameAndPartitionId(tableName,
          PARQUET_NORMALIZED_WRITE_TIME, writeType.name(), hostName, partitionId), writeTimePerRecordInUSec);
      observabilityRegistry.get().add(getWriteMetricWithTypeHostNameAndPartitionId(tableName,
          PARQUET_CUMULATIVE_WRITE_TIME, writeType.name(), hostName, partitionId), cumulativeWriteTimeInUsec);
      observabilityRegistry.get().add(getWriteMetricWithTypeHostNameAndPartitionId(tableName, TOTAL_RECORDS_WRITTEN,
          writeType.name(), hostName, partitionId), totalRecs);
      observabilityRegistry.get().add(getWriteMetricWithTypeHostNameAndPartitionId(tableName,
          PARQUET_WRITE_TIME_PER_MB_IN_USEC, writeType.name(), hostName, partitionId), writeTimePerMBInUSec);
      observabilityRegistry.get().add(getWriteMetricWithTypeHostNameAndPartitionId(tableName,
          PARQUET_WRITE_THROUGHPUT_MBPS, writeType.name(), hostName, partitionId), writeThroughputMBps);

      // consolidated stats from all executors
      observabilityRegistry.get().add(getConsolidatedWriteMetricKey(tableName,
          TOTAL_RECORDS_WRITTEN, writeType.name()), totalRecs);
      observabilityRegistry.get().add(getConsolidatedWriteMetricKey(tableName,
          PARQUET_CUMULATIVE_WRITE_TIME, writeType.name()), cumulativeWriteTimeInUsec);

    }
  }
}