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
 
package org.apache.xtable.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import lombok.extern.log4j.Log4j2;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Serves the bundled web UI.
 *
 * <p>The assets live under {@code src/main/resources/ui/} rather than {@code META-INF/resources/},
 * which Quarkus would serve automatically. Routing them through this resource is what makes {@code
 * xtable.ui.enabled} an actual gate instead of an advisory setting: with the flag off there is no
 * path that returns them.
 *
 * <p>Only a fixed set of file names is served, so a request cannot walk out of the asset directory.
 */
@Log4j2
@Path("/ui")
public class UiResource {

  private static final String ASSET_ROOT = "ui/";
  private static final String INDEX = "index.html";

  @Inject ConversionServiceConfig serviceConfig;

  @GET
  public Response index() {
    return asset(INDEX);
  }

  @GET
  @Path("/{file}")
  public Response file(@PathParam("file") String file) {
    return asset(file);
  }

  private Response asset(String file) {
    if (!serviceConfig.isUiEnabled()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    String mediaType = mediaTypeFor(file);
    if (mediaType == null) {
      // Unknown extension: refuse rather than guess, so this cannot become a file server.
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    try (InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(ASSET_ROOT + file)) {
      if (in == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok(readAll(in), mediaType).build();
    } catch (IOException e) {
      log.error("Failed to read UI asset {}", file, e);
      return Response.serverError().build();
    }
  }

  /** Allowlist of servable extensions; anything else is not found. */
  private static String mediaTypeFor(String file) {
    if (file.endsWith(".html")) {
      return MediaType.TEXT_HTML;
    }
    if (file.endsWith(".css")) {
      return "text/css";
    }
    if (file.endsWith(".js")) {
      return "application/javascript";
    }
    return null;
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
