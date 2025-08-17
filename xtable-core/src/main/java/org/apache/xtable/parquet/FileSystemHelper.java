package org.apache.xtable.parquet;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class FileSystemHelper {

  private static final FileSystemHelper INSTANCE = new FileSystemHelper();

  public static FileSystemHelper getInstance() {
    return INSTANCE;
  }

  public Stream<LocatedFileStatus> getParquetFiles(Configuration hadoopConf, String basePath) {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(basePath), true);
      return remoteIteratorToStream(iterator)
          .filter(file -> file.getPath().getName().endsWith("parquet"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, List<String>> getPartitionFromDirectoryStructure(
      Configuration hadoopConf, String basePath, Map<String, List<String>> partitionMap) {

    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      FileStatus[] baseFileStatus = fs.listStatus(new Path(basePath));
      Map<String, List<String>> currentPartitionMap = new HashMap<>(partitionMap);

      for (FileStatus dirStatus : baseFileStatus) {
        if (dirStatus.isDirectory()) {
          String partitionPath = dirStatus.getPath().getName();
          if (partitionPath.contains("=")) {
            String[] partitionKeyValue = partitionPath.split("=");
            currentPartitionMap
                .computeIfAbsent(partitionKeyValue[0], k -> new ArrayList<>())
                .add(partitionKeyValue[1]);
            getPartitionFromDirectoryStructure(
                hadoopConf, dirStatus.getPath().toString(), partitionMap);
          }
        }
      }
      return currentPartitionMap;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> Stream<T> remoteIteratorToStream(RemoteIterator<T> remoteIterator) {
    Iterator<T> iterator =
        new Iterator<T>() {
          @Override
          public boolean hasNext() {
            try {
              return remoteIterator.hasNext();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public T next() {
            try {
              return remoteIterator.next();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }
}
