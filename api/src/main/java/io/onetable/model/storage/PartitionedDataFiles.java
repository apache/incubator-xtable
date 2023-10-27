package io.onetable.model.storage;

import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.stat.Range;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a grouping of {@link OneDataFile} with the same partition values.
 */
@Value
@AllArgsConstructor(staticName = "of")
public class PartitionedDataFiles {
  List<PartitionFileGroup> partitions;

  @Value
  @Builder
  public static class PartitionFileGroup {
    Map<OnePartitionField, Range> partitionValues;
    List<OneDataFile> files;
  }

  public List<OneDataFile> getAllFiles() {
    return partitions.stream()
        .map(PartitionFileGroup::getFiles)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public static PartitionedDataFiles fromFiles(List<OneDataFile> files) {
    return fromFiles(files.stream());
  }

  public static PartitionedDataFiles fromFiles(Stream<OneDataFile> files) {
    Map<Map<OnePartitionField, Range>, List<OneDataFile>> filesGrouped =
        files
            .collect(
                Collectors.groupingBy(
                    OneDataFile::getPartitionValues));
    return PartitionedDataFiles.of(
        filesGrouped.entrySet().stream().map(entry -> PartitionFileGroup.builder().partitionValues(entry.getKey()).files(entry.getValue()).build()).collect(Collectors.toList()));
  }
}
