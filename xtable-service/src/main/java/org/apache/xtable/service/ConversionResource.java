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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.xtable.service.models.ConversionRunView;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.RunEvent;
import org.apache.xtable.service.models.SubmittedConversionResponse;

import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/v1/conversion")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConversionResource {

  /** Value of the {@code Prefer} header that switches a submit to asynchronous handling. */
  private static final String PREFER_RESPOND_ASYNC = "respond-async";

  @Inject ConversionService conversionService;

  @Inject AsyncConversionService asyncConversionService;

  @Inject ConversionRunStore runStore;

  /**
   * Converts a table.
   *
   * <p>Without a {@code Prefer} header this behaves exactly as it always has: the conversion runs
   * on the request thread and the final result is returned. With {@code Prefer: respond-async} the
   * conversion is scheduled and a {@code 202} carrying a {@code conversion-id} is returned
   * immediately, to be polled with {@link #getConversionStatus(String)}.
   *
   * <p>Both shapes are already described in {@code spec/rest-service-open-api.yaml}.
   */
  @POST
  @Path("/table")
  @Blocking
  public Response convertTable(
      @HeaderParam("Prefer") String prefer, ConvertTableRequest convertTableRequest) {
    if (isRespondAsync(prefer)) {
      ConversionRun run = asyncConversionService.submit(convertTableRequest);
      return Response.accepted(
              SubmittedConversionResponse.builder().conversionId(run.getConversionId()).build())
          .build();
    }
    ConvertTableResponse response = conversionService.convertTable(convertTableRequest);
    return Response.ok(response).build();
  }

  /**
   * Polls an asynchronous conversion.
   *
   * @return {@code 202} while the conversion is still running, {@code 200} with the result once it
   *     has succeeded, {@code 200} with the error detail once it has failed, or {@code 404} if the
   *     id is unknown or has been evicted from the bounded history
   */
  @GET
  @Path("/table/{conversion-id}")
  public Response getConversionStatus(@PathParam("conversion-id") String conversionId) {
    Optional<ConversionRun> run = runStore.get(conversionId);
    if (!run.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    ConversionRun conversionRun = run.get();
    if (!conversionRun.isFinished()) {
      return Response.status(Response.Status.ACCEPTED).build();
    }
    return Response.ok(conversionRun.toDetailView()).build();
  }

  /** Lists retained conversion runs, newest first. */
  @GET
  @Path("/runs")
  public List<ConversionRunView> listRuns() {
    return runStore.list().stream().map(ConversionRun::toSummaryView).collect(Collectors.toList());
  }

  /** Returns one run including its result, whether or not it has finished. */
  @GET
  @Path("/runs/{conversion-id}")
  public Response getRun(@PathParam("conversion-id") String conversionId) {
    return runStore
        .get(conversionId)
        .map(run -> Response.ok(run.toDetailView()).build())
        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
  }

  /**
   * Returns progress events for a run.
   *
   * <p>Clients poll this with the highest {@code sequence} they have already seen, so each call
   * returns only new events.
   */
  @GET
  @Path("/runs/{conversion-id}/events")
  public Response getRunEvents(
      @PathParam("conversion-id") String conversionId,
      @QueryParam("after") @DefaultValue("0") long after) {
    Optional<ConversionRun> run = runStore.get(conversionId);
    if (!run.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    List<RunEvent> events = run.get().eventsAfter(after);
    return Response.ok(events).build();
  }

  private static boolean isRespondAsync(String prefer) {
    return prefer != null && prefer.toLowerCase().contains(PREFER_RESPOND_ASYNC);
  }
}
