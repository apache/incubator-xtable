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
 
package org.apache.xtable.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetDataManager {

  private static final Configuration CONF = new Configuration();

  // Helper method to create a mock LocatedFileStatus
  private static LocatedFileStatus createMockFileStatus(String name, long modTime) {
    LocatedFileStatus mockStatus = mock(LocatedFileStatus.class);
    org.apache.hadoop.fs.Path mockPath = mock(org.apache.hadoop.fs.Path.class);
    when(mockPath.getName()).thenReturn(name);
    when(mockStatus.getPath()).thenReturn(mockPath);
    when(mockStatus.getModificationTime()).thenReturn(modTime);
    when(mockStatus.getLen()).thenReturn(1024L);
    return mockStatus;
  }

  private static class StubbedRemoteIterator implements RemoteIterator<LocatedFileStatus> {
    private final Iterator<LocatedFileStatus> files;

    public StubbedRemoteIterator(List<LocatedFileStatus> files) {
      this.files = files.iterator();
    }

    @Override
    public boolean hasNext() {
      return files.hasNext();
    }

    @Override
    public LocatedFileStatus next() {
      return files.next();
    }
  }

  // Helper method to create a mock FileSystem
  private static FileSystem createMockFileSystem(RemoteIterator<LocatedFileStatus> iterator)
      throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listFiles(any(org.apache.hadoop.fs.Path.class), anyBoolean())).thenReturn(iterator);
    return mockFs;
  }

  @Test
  void testGetMostRecentParquetFile_withMultipleFiles() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 3000L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 2000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    ParquetFileInfo result = manager.getMostRecentParquetFile();

    assertNotNull(result);
    assertEquals(3000L, result.getModificationTime());
  }

  @Test
  void testGetMostRecentParquetFile_noFiles() throws IOException {
    RemoteIterator<LocatedFileStatus> iterator = new StubbedRemoteIterator(new ArrayList<>());
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, manager::getMostRecentParquetFile);
    assertEquals("No files found", exception.getMessage());
  }

  @Test
  void testGetParquetDataFileAt_exactMatch() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    ParquetFileInfo result = manager.getParquetDataFileAt(2000L);

    assertNotNull(result);
    assertEquals(2000L, result.getModificationTime());
  }

  @Test
  void testGetParquetDataFileAt_firstAfterTime() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2500L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    ParquetFileInfo result = manager.getParquetDataFileAt(2000L);

    assertNotNull(result);
    assertEquals(2500L, result.getModificationTime());
  }

  @Test
  void testGetParquetDataFileAt_multipleAfterTime() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2500L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    ParquetFileInfo result = manager.getParquetDataFileAt(2000L);

    assertNotNull(result);
    assertEquals(2500L, result.getModificationTime());
  }

  @Test
  void testGetParquetDataFileAt_noMatch() throws IOException {
    RemoteIterator<LocatedFileStatus> iterator = new StubbedRemoteIterator(Collections.emptyList());
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> manager.getParquetDataFileAt(5000L));
    assertTrue(exception.getMessage().contains("No file found at or after 5000"));
  }

  @Test
  void testGetParquetDataFileAt_allBefore() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> manager.getParquetDataFileAt(3000L));
    assertTrue(exception.getMessage().contains("No file found at or after 3000"));
  }

  @Test
  void testGetCurrentFileInfo_multipleFiles() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    Stream<ParquetFileInfo> result = manager.getCurrentFileInfo();

    assertNotNull(result);
    List<ParquetFileInfo> fileList = result.collect(Collectors.toList());
    assertEquals(3, fileList.size());
    assertEquals(1000L, fileList.get(0).getModificationTime());
    assertEquals(2000L, fileList.get(1).getModificationTime());
    assertEquals(3000L, fileList.get(2).getModificationTime());
  }

  @Test
  void testGetCurrentFileInfo_emptyList() throws IOException {
    RemoteIterator<LocatedFileStatus> iterator = new StubbedRemoteIterator(Collections.emptyList());
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    Stream<ParquetFileInfo> result = manager.getCurrentFileInfo();

    assertNotNull(result);
    assertEquals(0, result.count());
  }

  @Test
  void testGetCurrentFileInfo_streamCharacteristics() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    Stream<ParquetFileInfo> result = manager.getCurrentFileInfo();

    assertNotNull(result);
    assertTrue(result.allMatch(info -> info.getModificationTime() > 0));
  }

  @Test
  void testGetParquetFilesMetadataAfterTime_someMatch() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    List<ParquetFileInfo> result = manager.getParquetFilesMetadataAfterTime(2000L);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals(2000L, result.get(0).getModificationTime());
    assertEquals(3000L, result.get(1).getModificationTime());
  }

  @Test
  void testGetParquetFilesMetadataAfterTime_allMatch() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 2000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    List<ParquetFileInfo> result = manager.getParquetFilesMetadataAfterTime(1000L);

    assertNotNull(result);
    assertEquals(2, result.size());
  }

  @Test
  void testGetParquetFilesMetadataAfterTime_noneMatch() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    List<ParquetFileInfo> result = manager.getParquetFilesMetadataAfterTime(5000L);

    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  void testGetParquetFilesMetadataAfterTime_exactTimeMatch() throws IOException {
    LocatedFileStatus file1 = createMockFileStatus("file1.parquet", 1000L);
    LocatedFileStatus file2 = createMockFileStatus("file2.parquet", 2000L);
    LocatedFileStatus file3 = createMockFileStatus("file3.parquet", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(file1, file2, file3));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    List<ParquetFileInfo> result = manager.getParquetFilesMetadataAfterTime(2000L);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals(2000L, result.get(0).getModificationTime());
    assertEquals(3000L, result.get(1).getModificationTime());
  }

  @Test
  void testGetParquetFiles_caching() throws IOException {
    LocatedFileStatus file = createMockFileStatus("file.parquet", 1000L);
    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Collections.singletonList(file));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    // No filesystem access should happen yet
    verify(mockFs, never()).listFiles(any(org.apache.hadoop.fs.Path.class), anyBoolean());

    // Access multiple times
    manager.getCurrentFileInfo();
    manager.getMostRecentParquetFile();
    manager.getParquetFilesMetadataAfterTime(0L);

    // Verify filesystem was accessed only once
    verify(mockFs, times(1)).listFiles(any(org.apache.hadoop.fs.Path.class), anyBoolean());
  }

  @Test
  void testOnlyParquetFilesIncluded() throws IOException {
    LocatedFileStatus parquetFile = createMockFileStatus("data.parquet", 1000L);
    LocatedFileStatus txtFile = createMockFileStatus("readme.txt", 2000L);
    LocatedFileStatus jsonFile = createMockFileStatus("config.json", 3000L);

    RemoteIterator<LocatedFileStatus> iterator =
        new StubbedRemoteIterator(Arrays.asList(parquetFile, txtFile, jsonFile));
    FileSystem mockFs = createMockFileSystem(iterator);

    ParquetDataManager manager = new ParquetDataManager(CONF, "test-path", mockFs);

    List<ParquetFileInfo> result = manager.getParquetFilesMetadataAfterTime(0L);

    assertEquals(1, result.size());
    assertEquals(1000L, result.get(0).getModificationTime());
  }

  @Test
  void testWithRealFileSystem_multipleFiles(@TempDir Path tempDir) throws IOException {
    // Create multiple parquet files with different modification times
    Path file1 = tempDir.resolve("data1.parquet");
    Path file2 = tempDir.resolve("data2.parquet");
    Path file3 = tempDir.resolve("data3.parquet");

    Files.createFile(file1);
    Files.createFile(file2);
    Files.createFile(file3);

    // Set different modification times
    Files.setLastModifiedTime(file1, FileTime.fromMillis(1000L));
    Files.setLastModifiedTime(file2, FileTime.fromMillis(3000L));
    Files.setLastModifiedTime(file3, FileTime.fromMillis(2000L));

    ParquetDataManager manager = new ParquetDataManager(CONF, tempDir.toString());

    ParquetFileInfo mostRecent = manager.getMostRecentParquetFile();
    assertNotNull(mostRecent);
    assertEquals(3000L, mostRecent.getModificationTime());

    List<ParquetFileInfo> afterTime = manager.getParquetFilesMetadataAfterTime(2000L);
    assertEquals(2, afterTime.size());
  }

  @Test
  void testWithRealFileSystem_nestedDirectories(@TempDir Path tempDir) throws IOException {
    // Create nested directory structure
    Path subDir1 = tempDir.resolve("subdir1");
    Path subDir2 = tempDir.resolve("subdir2");
    Files.createDirectories(subDir1);
    Files.createDirectories(subDir2);

    // Create parquet files in different directories
    Path file1 = tempDir.resolve("root.parquet");
    Path file2 = subDir1.resolve("nested1.parquet");
    Path file3 = subDir2.resolve("nested2.parquet");

    Files.createFile(file1);
    Files.createFile(file2);
    Files.createFile(file3);

    Files.setLastModifiedTime(file1, FileTime.fromMillis(1000L));
    Files.setLastModifiedTime(file2, FileTime.fromMillis(2000L));
    Files.setLastModifiedTime(file3, FileTime.fromMillis(3000L));
    ParquetDataManager manager = new ParquetDataManager(CONF, tempDir.toString());

    List<ParquetFileInfo> allFiles = manager.getParquetFilesMetadataAfterTime(0L);
    assertEquals(3, allFiles.size());

    ParquetFileInfo mostRecent = manager.getMostRecentParquetFile();
    assertEquals(3000L, mostRecent.getModificationTime());
  }

  @Test
  void testWithRealFileSystem_mixedFileTypes(@TempDir Path tempDir) throws IOException {
    // Create mix of parquet and non-parquet files
    Path parquetFile1 = tempDir.resolve("data1.parquet");
    Path parquetFile2 = tempDir.resolve("data2.parquet");
    Path txtFile = tempDir.resolve("readme.txt");
    Path jsonFile = tempDir.resolve("config.json");

    Files.createFile(parquetFile1);
    Files.createFile(parquetFile2);
    Files.createFile(txtFile);
    Files.createFile(jsonFile);

    Files.setLastModifiedTime(parquetFile1, FileTime.fromMillis(1000L));
    Files.setLastModifiedTime(parquetFile2, FileTime.fromMillis(2000L));
    Files.setLastModifiedTime(txtFile, FileTime.fromMillis(3000L));
    Files.setLastModifiedTime(jsonFile, FileTime.fromMillis(4000L));

    ParquetDataManager manager = new ParquetDataManager(CONF, tempDir.toString());

    List<ParquetFileInfo> allFiles = manager.getParquetFilesMetadataAfterTime(0L);
    // Only parquet files should be included
    assertEquals(2, allFiles.size());

    ParquetFileInfo mostRecent = manager.getMostRecentParquetFile();
    // Most recent parquet file, not the txt or json
    assertEquals(2000L, mostRecent.getModificationTime());
  }
}
