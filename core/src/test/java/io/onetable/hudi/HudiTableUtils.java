package io.onetable.hudi;

import java.util.UUID;

public class HudiTableUtils {
  public static String getTableName() {
    return "test-" + UUID.randomUUID();
  }
}
