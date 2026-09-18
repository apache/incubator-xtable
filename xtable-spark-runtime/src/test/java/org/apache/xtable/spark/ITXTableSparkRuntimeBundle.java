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
 
package org.apache.xtable.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;

/**
 * Validates that the relocated {@code xtable-spark-runtime} bundle jar is self-contained by running
 * an actual XTable sync via {@code $SPARK_HOME/bin/spark-submit} against a locally created Hudi
 * table. Skipped when {@code SPARK_HOME} is not set.
 */
class ITXTableSparkRuntimeBundle {

  @TempDir Path tempDir;

  @Test
  void bundleRunsHudiToIcebergSyncViaSparkSubmit() throws Exception {
    String sparkHome = System.getenv("SPARK_HOME");
    assumeTrue(sparkHome != null && !sparkHome.trim().isEmpty(), "SPARK_HOME is not set");

    String tableName = "orders";
    String basePath;
    try (GenericTable<?, ?> table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(10);
      basePath = table.getBasePath();
    }

    File bundleJar = findBundleJar();
    File sparkSubmit = new File(sparkHome, "bin/spark-submit");
    assertTrue(sparkSubmit.canExecute(), "spark-submit not executable at " + sparkSubmit);

    // Engines are provided by the user's runtime, not bundled. Supply Hudi + Iceberg (matching the
    // versions from the root pom) via --packages, exactly as a user would. Delta is intentionally
    // absent: resilient target discovery skips it.
    String enginePackages = System.getProperty("xtable.it.enginePackages");
    assertTrue(
        enginePackages != null && !enginePackages.isEmpty(),
        "xtable.it.enginePackages system property must be set by the failsafe plugin");

    ProcessBuilder pb =
        new ProcessBuilder(
            sparkSubmit.getAbsolutePath(),
            "--master",
            "local[2]",
            "--packages",
            enginePackages,
            "--class",
            "org.apache.xtable.spark.XTableSparkSync",
            bundleJar.getAbsolutePath(),
            "--basePath",
            basePath,
            "--sourceFormat",
            "HUDI",
            "--targets",
            "ICEBERG",
            "--tableName",
            tableName);
    pb.environment().put("SPARK_LOCAL_IP", "127.0.0.1");
    pb.redirectErrorStream(true);

    Process process = pb.start();
    String output = readOutput(process);
    boolean finished = process.waitFor(10, TimeUnit.MINUTES);
    if (!finished) {
      process.destroyForcibly();
      throw new AssertionError("spark-submit timed out. Output:\n" + output);
    }
    int exit = process.exitValue();
    assertEquals(0, exit, "spark-submit failed (exit " + exit + "). Output:\n" + output);

    File metadataDir = new File(new File(URI.create(basePath)), "metadata");
    File[] metadataJson = metadataDir.listFiles((dir, name) -> name.endsWith("metadata.json"));
    assertTrue(
        metadataJson != null && metadataJson.length > 0,
        "No Iceberg metadata produced under " + metadataDir + ". spark-submit output:\n" + output);
  }

  private File findBundleJar() {
    File targetDir = new File("target");
    File[] candidates =
        targetDir.listFiles(
            (dir, name) -> name.startsWith("xtable-spark-runtime") && name.endsWith("-bundle.jar"));
    assertTrue(
        candidates != null && candidates.length == 1,
        "Expected exactly one bundle jar in "
            + targetDir.getAbsolutePath()
            + " (run the package phase first)");
    return candidates[0];
  }

  private static String readOutput(Process process) throws Exception {
    StringBuilder sb = new StringBuilder();
    try (java.io.BufferedReader reader =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(
                process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append(System.lineSeparator());
      }
    }
    return sb.toString();
  }
}
