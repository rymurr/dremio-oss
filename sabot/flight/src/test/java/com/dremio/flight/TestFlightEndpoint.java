/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.flight;

import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.flight.formation.FormationConfig;
import com.dremio.proto.flight.commands.Command;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.typesafe.config.ConfigValueFactory;
import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.users.SystemUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Basic flight endpoint test
 */
public class TestFlightEndpoint extends BaseTestQuery {

  private static InitializerRegistry registry;
  private static final LinkedBuffer buffer = LinkedBuffer.allocate();
  private static final ExecutorService tp = Executors.newFixedThreadPool(4);
  private static final Logger logger = LoggerFactory.getLogger(TestFlightEndpoint.class);

  @BeforeClass
  public static void init() throws Exception {
    BaseTestQuery.updateTestCluster(1, config.withValue("dremio.test.query.printing.silent", ConfigValueFactory.fromAnyRef(false)));
    registry = new InitializerRegistry(ExecTest.CLASSPATH_SCAN_RESULT, getBindingProvider());
    registry.start();
    SourceConfig c = new SourceConfig();
    FormationConfig conf = new FormationConfig();
    c.setConnectionConf(conf);
    c.setName("flight");
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    CatalogServiceImpl cserv = (CatalogServiceImpl) getBindingProvider().lookup(CatalogService.class);
    cserv.createSourceIfMissingWithThrow(c);

  }

  @AfterClass
  public static void shutdown() throws Exception {
    registry.close();
  }

  @Test
  public void connect() throws Exception {
    testNoResult("alter session set \"planner.slice_target\" = 10");
    FlightClient c = FlightClient.builder().allocator(getAllocator()).location(Location.forGrpcInsecure("localhost", 47470)).build();
    c.authenticateBasic(SystemUser.SYSTEM_USERNAME, null);
    String sql = "select * from dfs.\"/home/ryan/workspace/tick_data/data\"";
    String easySql = "select * from sys.options";
    String hardSql = "select * from dfs.\"/home/ryan/workspace/tick_data/data\"";
    byte[] message = ProtostuffIOUtil.toByteArray(new Command(hardSql, false, false, ByteString.EMPTY), Command.getSchema(), buffer);
    buffer.clear();
    FlightInfo info = c.getInfo(FlightDescriptor.command(message));
    long total = info.getEndpoints().stream()
      .map(this::submit)
      .map(TestFlightEndpoint::get)
      .mapToLong(Long::longValue)
      .sum();

    c.close();
    System.out.println(total);

  }

  @Test
  public void connectParallel() throws Exception {
    logger.debug("starting!");
//    testNoResult("alter session set \"planner.slice_target\" = 10");
    FlightClient c = FlightClient.builder().allocator(getAllocator()).location(Location.forGrpcInsecure("localhost", 47470)).build();
    c.authenticateBasic(SystemUser.SYSTEM_USERNAME, null);
    String sql = "select * from dfs.\"/home/ryan/workspace/dremio-oss/sabot/kernel/src/test/resources/test123\"";
    String hardSql = "select * from dfs.\"/home/ryan/workspace/tick_data/data\"";
    String easySql = "select * from sys.options";
    logger.debug("sending get info message");
    byte[] message = ProtostuffIOUtil.toByteArray(new Command(hardSql, true, false, ByteString.EMPTY), Command.getSchema(), buffer);
    buffer.clear();
    FlightInfo info = c.getInfo(FlightDescriptor.command(message));
    logger.debug("received get info message");
    message = ProtostuffIOUtil.toByteArray(new Command("", true, true, ByteString.copyFrom(info.getEndpoints().get(0).getTicket().getBytes())), Command.getSchema(), buffer);
    buffer.clear();
    logger.debug("sending coalesce message");
    FlightInfo finalInfo = c.getInfo(FlightDescriptor.command(message));

    AtomicInteger endpointCount = new AtomicInteger();
    logger.debug("received coalesce message with {} endpoints", finalInfo.getEndpoints().size());
    finalInfo.getEndpoints().forEach(e -> {
      logger.debug("Endpoint {} of {}. Ticket is {}, uri is {}", endpointCount.incrementAndGet(), finalInfo.getEndpoints().size(), new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
    });
    ExecutorService executorService = Executors.newFixedThreadPool(24);
    CompletionService<Long> completionService =
      new ExecutorCompletionService<>(executorService);
    int remainingFutures = 0;
    long total = 0;
    long totalCount = 0;
    for (FlightEndpoint e: finalInfo.getEndpoints()) {
      int thisEndpoint = endpointsSubmitted.incrementAndGet();
      logger.debug("submitting flight endpoint {} with ticket {} to {}", thisEndpoint, new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
      RunnableReader reader = new RunnableReader(allocator, e);
      completionService.submit(reader);
      logger.debug("submitted flight endpoint {} with ticket {} to {}", thisEndpoint, new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
      remainingFutures++;
    }

    while (remainingFutures > 0) {
      Future<Long> completedFuture = completionService.take();
      remainingFutures--;
      Long l = completedFuture.get();
//      Long l = reader.call();
      total += l;
      totalCount++;
      logger.info("returned future {} of {} with value {}", endpointsReceived.incrementAndGet(), endpointsSubmitted.get(), l);
      logger.error("total so far is {} after {} futures", total, totalCount);
      logger.error("We are waiting on {} futures", remainingFutures);
    }
    long expected = 13460172;

    c.close();

    System.out.println(total + "  " + expected);
  }

  private static AtomicInteger endpointsSubmitted = new AtomicInteger();
  private static AtomicInteger endpointsWaitingOn = new AtomicInteger();
  private static AtomicInteger endpointsReceived = new AtomicInteger();

  private Future<Long> submit(FlightEndpoint e) {
    int thisEndpoint = endpointsSubmitted.incrementAndGet();
    logger.debug("submitting flight endpoint {} with ticket {} to {}", thisEndpoint, new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
    RunnableReader reader = new RunnableReader(allocator, e);
    Future<Long> f = tp.submit(reader);
    logger.debug("submitted flight endpoint {} with ticket {} to {}", thisEndpoint, new String(e.getTicket().getBytes()), e.getLocations().get(0).getUri());
    return f;
  }

  private static Long get(Future<Long> r) {
    try {
      logger.debug("starting wait on future {} of {}", endpointsWaitingOn.incrementAndGet(), endpointsSubmitted.get());
      Long f = r.get();
      logger.debug("returned future {} of {} with value {}", endpointsReceived.incrementAndGet(), endpointsSubmitted.get(), f);
      return f;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }


  private static class RunnableReader implements Callable<Long> {
    private final BufferAllocator allocator;
    private FlightEndpoint endpoint;

    private RunnableReader(BufferAllocator allocator, FlightEndpoint endpoint) {
      this.allocator = allocator;
      this.endpoint = endpoint;
    }

    @Override
    public Long call() {
      long count = 0;
      int readIndex = 0;
      logger.debug("starting work on flight endpoint with ticket {} to {}", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri());
      try (FlightClient c = FlightClient.builder().allocator(allocator).location(endpoint.getLocations().get(0)).build()) {
        c.authenticateBasic(SystemUser.SYSTEM_USERNAME, null);
        logger.debug("trying to get stream for flight endpoint with ticket {} to {}", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri());
        FlightStream fs = c.getStream(endpoint.getTicket());
        logger.debug("got stream for flight endpoint with ticket {} to {}. Will now try and read", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri());
        while (fs.next()) {
          long thisCount = fs.getRoot().getRowCount();
          count += thisCount;
          logger.debug("got results from stream for flight endpoint with ticket {} to {}. This is read {} and we got {} rows back for a total of {}", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri(), ++readIndex, thisCount, count);
          fs.getRoot().clear();
        }
      } catch (InterruptedException e) {

      } catch (Throwable t) {
        logger.error("WTF", t);
      }
      logger.debug("got all results from stream for flight endpoint with ticket {} to {}. We read {} batches and we got {} rows back", new String(endpoint.getTicket().getBytes()), endpoint.getLocations().get(0).getUri(), ++readIndex, count);
      return count;
    }
  }
}
