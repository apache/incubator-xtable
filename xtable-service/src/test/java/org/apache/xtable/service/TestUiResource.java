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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.ws.rs.core.Response;

@ExtendWith(MockitoExtension.class)
class TestUiResource {

  @Mock private ConversionServiceConfig serviceConfig;

  @InjectMocks private UiResource resource;

  /**
   * The gate matters: the service ships with no authentication, so an existing deployment must not
   * gain a browser interface just by upgrading.
   */
  @Test
  void assetsAreNotServedWhenTheUiIsDisabled() {
    when(serviceConfig.isUiEnabled()).thenReturn(false);

    assertEquals(404, resource.index().getStatus());
    assertEquals(404, resource.file("app.js").getStatus());
    assertEquals(404, resource.file("styles.css").getStatus());
  }

  @Test
  void assetsAreServedWithTheirMediaTypeWhenEnabled() {
    when(serviceConfig.isUiEnabled()).thenReturn(true);

    Response index = resource.index();
    assertEquals(200, index.getStatus());
    assertEquals("text/html", index.getMediaType().toString());
    assertTrue(new String((byte[]) index.getEntity()).contains("<title>"));

    assertEquals("application/javascript", resource.file("app.js").getMediaType().toString());
    assertEquals("text/css", resource.file("styles.css").getMediaType().toString());
  }

  /** Only html, css and js are servable, so this cannot become a general file server. */
  @Test
  void otherExtensionsAreRefusedEvenWhenEnabled() {
    when(serviceConfig.isUiEnabled()).thenReturn(true);

    assertEquals(404, resource.file("application.properties").getStatus());
    assertEquals(404, resource.file("xtable-hadoop-defaults.xml").getStatus());
  }

  @Test
  void missingAssetIsNotFound() {
    when(serviceConfig.isUiEnabled()).thenReturn(true);

    assertEquals(404, resource.file("does-not-exist.js").getStatus());
  }
}
