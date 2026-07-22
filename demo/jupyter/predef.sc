/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Kernel predef for the XTable demo notebooks: loads every dependency the
// notebooks need when the kernel starts, so the notebooks themselves contain
// no dependency management. The artifacts are already present in the image's
// coursier cache (pre-fetched at image build time); versions come from
// /home/jars/versions.properties, generated from the root pom by
// build_demo.sh.

val xtableVersions = {
  val props = new java.util.Properties()
  val in = new java.io.FileInputStream("/home/jars/versions.properties")
  try props.load(in)
  finally in.close()
  props
}

def xtableVersion(key: String): String =
  Option(xtableVersions.getProperty(key)).map(_.trim).filter(_.nonEmpty).getOrElse {
    sys.error(s"$key missing from /home/jars/versions.properties, re-run build_demo.sh")
  }

{
  val scalaBinaryVersion = xtableVersion("scala.binary.version")
  val sparkVersionPrefix = xtableVersion("spark.version.prefix")

  Seq(
    ("org.apache.logging.log4j", "log4j-api", "2.17.2"),
    ("org.apache.logging.log4j", "log4j-core", "2.17.2"),
    ("org.apache.spark", s"spark-sql_$scalaBinaryVersion", xtableVersion("spark.version")),
    ("org.apache.spark", s"spark-hive_$scalaBinaryVersion", xtableVersion("spark.version")),
    ("org.apache.hudi", s"hudi-spark$sparkVersionPrefix-bundle_$scalaBinaryVersion", xtableVersion("hudi.version")),
    ("org.apache.hudi", "hudi-java-client", xtableVersion("hudi.version")),
    ("io.delta", s"delta-core_$scalaBinaryVersion", xtableVersion("delta.version")),
    ("io.delta", "delta-kernel-api", xtableVersion("delta.kernel.version")),
    ("io.delta", "delta-kernel-defaults", xtableVersion("delta.kernel.version")),
    ("org.apache.iceberg", "iceberg-hive-runtime", xtableVersion("iceberg.hive.runtime.version")),
    ("io.trino", "trino-jdbc", "431"),
    ("com.facebook.presto", "presto-jdbc", "0.283")
  ).foreach { case (org, name, version) =>
    interp.load.ivy(coursierapi.Dependency.of(org, name, version))
  }

  // Load the xtable jars built by build_demo.sh (version-agnostic).
  new java.io.File("/home/jars").listFiles
    .filter(_.getName.endsWith(".jar"))
    .foreach(jar => interp.load.cp(os.Path(jar.getAbsolutePath)))
}

println("XTable demo kernel ready: Spark, Hudi, Delta, Iceberg and the XTable jars are on the classpath.")
