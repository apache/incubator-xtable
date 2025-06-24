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
 
package org.apache.xtable;

// import org.junit.jupiter.api.Test;
//
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.*;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

public class DeltaTableKernel {
  private static final Logger logger = LoggerFactory.getLogger(DeltaTableKernel.class);

  @Test
  public void readDeltaKernel() throws IOException {
    String myTablePath =
        "/Users/vaibhakumar/Desktop/opensource/iceberg/warehouse/demo/nyc/taxis"; // fully qualified
    Configuration hadoopConf = new Configuration();
    Engine myEngine = DefaultEngine.create(hadoopConf);
    Table myTable = Table.forPath(myEngine, myTablePath);
    Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);
    long version = mySnapshot.getVersion();
    StructType tableSchema = mySnapshot.getSchema();
    Scan myScan = mySnapshot.getScanBuilder().build();

    // Common information about scanning for all data files to read.
    Row scanState = myScan.getScanState(myEngine);

    // Information about the list of scan files to read
    CloseableIterator<FilteredColumnarBatch> fileIter = myScan.getScanFiles(myEngine);
    int readRecordCount = 0;
    try {
      StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(myEngine, scanState);
      while (fileIter.hasNext()) {
        FilteredColumnarBatch scanFilesBatch = fileIter.next();
        try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
          while (scanFileRows.hasNext()) {
            Row scanFileRow = scanFileRows.next();
            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            CloseableIterator<ColumnarBatch> physicalDataIter =
                myEngine
                    .getParquetHandler()
                    .readParquetFiles(
                        singletonCloseableIterator(fileStatus),
                        physicalReadSchema,
                        Optional.empty());
            try (CloseableIterator<FilteredColumnarBatch> transformedData =
                Scan.transformPhysicalData(myEngine, scanState, scanFileRow, physicalDataIter)) {
              while (transformedData.hasNext()) {
                FilteredColumnarBatch logicalData = transformedData.next();
                ColumnarBatch dataBatch = logicalData.getData();

                // access the data for the column at ordinal 0
                ColumnVector column0 = dataBatch.getColumnVector(0);
                ColumnVector column1 = dataBatch.getColumnVector(1);
                ColumnVector column2 = dataBatch.getColumnVector(2);
                ColumnVector column3 = dataBatch.getColumnVector(3);

                for (int rowIndex = 0; rowIndex < column0.getSize(); rowIndex++) {
                  System.out.println(column0.getInt(rowIndex));
                }
                for (int rowIndex = 0; rowIndex < column1.getSize(); rowIndex++) {
                  System.out.println(column1.getString(rowIndex));
                }
              }
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("IOException occurred: " + e.getMessage());
    }
  }
}
