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
import io.delta.kernel.*;
 import io.delta.kernel.defaults.*;
// import org.apache.hadoop.conf.Configuration;

public class DeltaTableKernel {
  //    @Test
  public void readDeltaKernel() {
    //        String myTablePath
    // ="/Users/vaibhakumar/Desktop/opensource/iceberg/warehouse/demo/nyc/taxis"; // fully qualified
    // table path. Ex: file:/user/tables/myTable
    //        Configuration hadoopConf = new Configuration();
    //        Engine myEngine = DefaultEngine.create(hadoopConf);
    //        Table myTable = Table.forPath(myEngine, myTablePath);
    //        Snapshot mySnapshot = myTable.getLatestSnapshot(myEngine);
    //        long version = mySnapshot.getVersion();
    //        StructType tableSchema = mySnapshot.getSchema();
    //        Scan myScan = mySnapshot.getScanBuilder(myEngine).build();

    // Common information about scanning for all data files to read.
    //        Row scanState = myScan.getScanState(myEngine);

    // Information about the list of scan files to read
    //        CloseableIterator<FilteredColumnarBatch> scanFiles = myScan.getScanFiles(myEngine);
  }
}
