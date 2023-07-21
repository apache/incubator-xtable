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
 
package io.onetable.spi;

import java.util.HashMap;
import java.util.Map;

import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;

/**
 * Default implementation of the {@link OneTableSnapshotVisitor}
 *
 * @since 0.1
 */
public abstract class DefaultSnapshotVisitor implements OneTableSnapshotVisitor {
  public static Map<String, OneDataFile> extractDataFilePaths(OneDataFiles dataFiles) {
    Map<String, OneDataFile> paths = new HashMap<>();
    dataFiles.acceptVisitor(
        new DefaultSnapshotVisitor() {
          @Override
          protected void visitSelf(OneDataFile dataFile) {
            paths.put(dataFile.getPhysicalPath(), dataFile);
          }
        });
    return paths;
  }

  @Override
  public void visit(OneSnapshot snapshot) {
    this.visitSelf(snapshot);
    this.visit(snapshot.getTable());
    this.visit(snapshot.getSchemaCatalog());
    this.visit(snapshot.getDataFiles());
  }

  private void visitSelf(OneSnapshot snapshot) {}

  @Override
  public void visit(OneTable table) {
    this.visitSelf(table);
    this.visit(table.getReadSchema());
  }

  private void visitSelf(OneTable table) {}

  @Override
  public void visit(SchemaCatalog schemaCatalog) {
    this.visitSelf(schemaCatalog);
  }

  private void visitSelf(SchemaCatalog schemaCatalog) {}

  @Override
  public void visit(OneDataFiles collection) {
    this.visitSelf(collection);
    collection
        .getFiles()
        .forEach(
            f -> {
              if (f instanceof OneDataFiles) {
                this.visit((OneDataFiles) f);
              } else {
                this.visit((OneDataFile) f);
              }
            });
  }

  protected void visitSelf(OneDataFiles collection) {}

  @Override
  public void visit(OneDataFile dataFile) {
    this.visitSelf(dataFile);
  }

  protected void visitSelf(OneDataFile dataFile) {}

  @Override
  public void visit(OneSchema tableReadSchema) {
    this.visitSelf(tableReadSchema);
  }

  private void visitSelf(OneSchema tableReadSchema) {}
}
