package io.onetable.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CustomCollectors {
  public static <T> Collector<T, ?, List<T>> toList(int size) {
    return Collectors.toCollection(() -> new ArrayList<>(size));
  }
}
