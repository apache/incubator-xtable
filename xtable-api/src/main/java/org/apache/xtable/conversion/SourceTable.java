package org.apache.xtable.conversion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import org.apache.hadoop.conf.Configuration;

@EqualsAndHashCode(callSuper = true)
@Getter
public class SourceTable extends ExternalTable {
  /** The path to the data files, defaults to the basePath */
  @NonNull private final String dataPath;

  @Builder(toBuilder = true)
  public SourceTable(
          String name,
          String formatName,
          String basePath,
          String dataPath,
          String[] namespace,
          CatalogConfig catalogConfig,
          Properties additionalProperties,
          Configuration hadoopConf) {
    super(
            name,
            formatName != null ? formatName : resolveFormatOrThrow(basePath, hadoopConf),
            basePath,
            namespace,
            catalogConfig,
            additionalProperties);
    this.dataPath = dataPath == null ? this.getBasePath() : sanitizeBasePath(dataPath);
  }
  
  private static String resolveFormatOrThrow(String basePath, Configuration hadoopConf) {
    try {
      return DetectSourceType.detectFormat(basePath, hadoopConf);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to auto-detect source table format", e);
    }
  }
}