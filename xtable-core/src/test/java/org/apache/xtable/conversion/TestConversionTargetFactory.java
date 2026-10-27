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
 
package org.apache.xtable.conversion;

import static org.apache.xtable.GenericTable.getTableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.delta.DeltaConversionTarget;
import org.apache.xtable.delta.DeltaConversionTargetConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.hudi.HudiConversionTarget;
import org.apache.xtable.iceberg.IcebergConversionTarget;
import org.apache.xtable.kernel.DeltaKernelConversionTarget;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

public class TestConversionTargetFactory {

  @Test
  public void testConversionTargetFromNameForDELTA() {
    ConversionTarget tc =
        ConversionTargetFactory.getInstance().createConversionTargetForName(TableFormat.DELTA);
    assertNotNull(tc);
    TargetTable targetTable = getPerTableConfig(TableFormat.DELTA);
    Configuration conf = new Configuration();
    conf.set("spark.master", "local");
    tc.init(targetTable, conf);
    assertEquals(tc.getTableFormat(), TableFormat.DELTA);
  }

  @Test
  public void testConversionTargetFromNameForHUDI() {
    ConversionTarget tc =
        ConversionTargetFactory.getInstance().createConversionTargetForName(TableFormat.HUDI);
    assertNotNull(tc);
    TargetTable targetTable = getPerTableConfig(TableFormat.HUDI);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(targetTable, conf);
    assertEquals(tc.getTableFormat(), TableFormat.HUDI);
  }

  @Test
  public void testConversionTargetFromNameForICEBERG() {
    ConversionTarget tc =
        ConversionTargetFactory.getInstance().createConversionTargetForName(TableFormat.ICEBERG);
    assertNotNull(tc);
    TargetTable targetTable = getPerTableConfig(TableFormat.ICEBERG);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    tc.init(targetTable, conf);
    assertEquals(tc.getTableFormat(), TableFormat.ICEBERG);
  }

  @Test
  public void testConversionTargetFromNameForUNKOWN() {
    NotSupportedException thrown =
        assertThrows(
            NotSupportedException.class,
            () -> ConversionTargetFactory.getInstance().createConversionTargetForName("UNKNOWN"),
            "NotSupportedException expected and operation succeeded inappropriately.");
    assertTrue(thrown.getMessage().contains("UNKNOWN"));
  }

  @Test
  public void testConversionTargetFromFormatType() {
    TargetTable targetTable = getPerTableConfig(TableFormat.DELTA);
    Configuration conf = new Configuration();
    conf.setStrings("spark.master", "local");
    ConversionTarget tc = ConversionTargetFactory.getInstance().createForFormat(targetTable, conf);
    assertEquals(tc.getTableFormat(), TableFormat.DELTA);
  }

  @Test
  public void testDeltaTargetDefaultsToStandalone() {
    // No properties and an empty properties set both resolve to the Delta Standalone target.
    assertInstanceOf(
        DeltaConversionTarget.class,
        ConversionTargetFactory.getInstance().createConversionTargetForName(TableFormat.DELTA));
    assertInstanceOf(
        DeltaConversionTarget.class,
        ConversionTargetFactory.getInstance()
            .createConversionTargetForName(TableFormat.DELTA, new Properties()));
  }

  @Test
  public void testDeltaTargetExplicitlyDisablingKernelUsesStandalone() {
    Properties properties = new Properties();
    properties.setProperty(DeltaConversionTargetConfig.USE_KERNEL, "false");
    assertInstanceOf(
        DeltaConversionTarget.class,
        ConversionTargetFactory.getInstance()
            .createConversionTargetForName(TableFormat.DELTA, properties));
  }

  @Test
  public void testDeltaTargetUsesKernelWhenFlagEnabled() {
    Properties properties = new Properties();
    properties.setProperty(DeltaConversionTargetConfig.USE_KERNEL, "true");
    assertInstanceOf(
        DeltaKernelConversionTarget.class,
        ConversionTargetFactory.getInstance()
            .createConversionTargetForName(TableFormat.DELTA, properties));
  }

  @Test
  public void testKernelFlagDoesNotAffectNonDeltaFormats() {
    Properties properties = new Properties();
    properties.setProperty(DeltaConversionTargetConfig.USE_KERNEL, "true");
    assertInstanceOf(
        HudiConversionTarget.class,
        ConversionTargetFactory.getInstance()
            .createConversionTargetForName(TableFormat.HUDI, properties));
  }

  @Test
  public void testDiscoversAvailableTargetWhenOtherEnginesAreMissing(@TempDir Path tempDir)
      throws Exception {
    ConversionTarget target =
        withEngineMissingClassLoader(
            tempDir,
            () ->
                ConversionTargetFactory.getInstance()
                    .createConversionTargetForName(TableFormat.ICEBERG));
    assertEquals(IcebergConversionTarget.class.getName(), target.getClass().getName());
  }

  @Test
  public void testMissingEngineForRequestedFormatFailsWithNotSupported(@TempDir Path tempDir) {
    NotSupportedException thrown =
        assertThrows(
            NotSupportedException.class,
            () ->
                withEngineMissingClassLoader(
                    tempDir,
                    () ->
                        ConversionTargetFactory.getInstance()
                            .createConversionTargetForName(TableFormat.DELTA)));
    assertTrue(thrown.getMessage().contains(TableFormat.DELTA));
  }

  /**
   * Runs the call with a context classloader that registers four providers: one whose class does
   * not exist, two Delta targets whose engine is absent (a missing superclass fails in hasNext(), a
   * missing constructor dependency fails in next()), and the Iceberg target. The timeout guards
   * against the provider iterator retrying a failing entry forever.
   */
  private static <T> T withEngineMissingClassLoader(Path tempDir, Callable<T> call)
      throws Exception {
    Path services = tempDir.resolve(ConversionTarget.class.getName());
    Files.write(
        services,
        Arrays.asList(
            "org.apache.xtable.missing.NoSuchConversionTarget",
            EngineSubclassDeltaTarget.class.getName(),
            EngineFieldDeltaTarget.class.getName(),
            IcebergConversionTarget.class.getName()),
        StandardCharsets.UTF_8);
    ClassLoader loader =
        new EngineMissingClassLoader(
            TestConversionTargetFactory.class.getClassLoader(), services.toUri().toURL());
    return assertTimeoutPreemptively(
        Duration.ofSeconds(30),
        () -> {
          Thread thread = Thread.currentThread();
          ClassLoader previous = thread.getContextClassLoader();
          thread.setContextClassLoader(loader);
          try {
            return call.call();
          } finally {
            thread.setContextClassLoader(previous);
          }
        });
  }

  /** Stands in for an engine class; hidden by {@link EngineMissingClassLoader}. */
  public static class DeltaEngine {}

  /** Stands in for an engine base class; hidden by {@link EngineMissingClassLoader}. */
  public static class DeltaEngineTarget extends DeltaConversionTarget {}

  /** A Delta target whose superclass is in the absent engine. */
  public static class EngineSubclassDeltaTarget extends DeltaEngineTarget {}

  /** A Delta target whose constructor needs the absent engine. */
  public static class EngineFieldDeltaTarget extends DeltaConversionTarget {
    private final DeltaEngine engine = new DeltaEngine();
  }

  /**
   * Serves a fixed ConversionTarget services file, hides the engine classes, and defines the
   * engine-backed targets itself so that their engine references resolve here.
   */
  private static class EngineMissingClassLoader extends ClassLoader {
    private static final String SERVICES_RESOURCE =
        "META-INF/services/" + ConversionTarget.class.getName();
    private static final List<String> HIDDEN =
        Arrays.asList(DeltaEngine.class.getName(), DeltaEngineTarget.class.getName());
    private static final List<String> CHILD_FIRST =
        Arrays.asList(
            EngineSubclassDeltaTarget.class.getName(), EngineFieldDeltaTarget.class.getName());

    private final URL services;

    EngineMissingClassLoader(ClassLoader parent, URL services) {
      super(parent);
      this.services = services;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      return SERVICES_RESOURCE.equals(name)
          ? Collections.enumeration(Collections.singletonList(services))
          : super.getResources(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        if (HIDDEN.contains(name)) {
          throw new ClassNotFoundException(name);
        }
        if (!CHILD_FIRST.contains(name)) {
          return super.loadClass(name, resolve);
        }
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          try (InputStream in =
              getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
            byte[] bytes = readAll(in);
            loaded = defineClass(name, bytes, 0, bytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }

    private static byte[] readAll(InputStream in) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }

  private TargetTable getPerTableConfig(String tableFormat) {
    return TargetTable.builder()
        .name(getTableName())
        .basePath("/tmp/doesnt/matter")
        .formatName(tableFormat)
        .build();
  }
}
