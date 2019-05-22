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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.datastore.KVStore;
import com.dremio.exec.physical.config.UnionExchange;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.beans.CoreOperatorType;
import com.dremio.flight.formation.FlightStoreCreator;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserBitShared.RecordBatchDef;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.PreparedStatement;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.ResultColumnMetadata;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.proto.flight.commands.Command;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;
import io.netty.buffer.ArrowBuf;
import io.protostuff.ProtostuffIOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Producer implements FlightProducer, AutoCloseable {
  private static final Joiner JOINER = Joiner.on(":");
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  private final Location location;
  private final Provider<UserWorker> worker;
  private final Provider<SabotContext> context;
  private final BufferAllocator allocator;
  private final AuthValidator validator;
  private final KVStore<FlightStoreCreator.NodeKey, FlightStoreCreator.NodeKey> kvStore;

  public Producer(Location location, Provider<UserWorker> worker, Provider<SabotContext> context, BufferAllocator allocator, AuthValidator validator) {
    super();
    this.location = location;
    this.worker = worker;
    this.context = context;
    this.allocator = allocator;
    this.validator = validator;
    kvStore = context.get().getKVStoreProvider().getStore(FlightStoreCreator.class);
  }

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> resultStreamListener) {
  }

  private FlightInfo getCoalesce(CallContext callContext, FlightDescriptor descriptor, Command cmd) {
    PrepareParallel d = new PrepareParallel(kvStore);
    logger.debug("coalescing query {}", new String(cmd.getTicket().toByteArray()));
    RunQuery query;
    try {
      PreparedStatementHandle handle = PreparedStatementHandle.PARSER.parseFrom(cmd.getTicket().toByteArray());
      query = RunQuery.newBuilder()
        .setType(QueryType.PREPARED_STATEMENT)
        .setPreparedStatementHandle(handle)
        .build();
    } catch (InvalidProtocolBufferException e) {
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }

    UserRequest request = new UserRequest(RpcType.RUN_QUERY, query);
    UserBitShared.ExternalId externalId = submitWork(callContext, request, d);
    String queryId = QueryIdHelper.getQueryId(ExternalIdHelper.toQueryId(externalId));
    logger.debug("submitted query for {} and got query id {}. Will now wait for parallel planner to return.", new String(cmd.getTicket().toByteArray()), queryId);
    return d.getInfo(descriptor, queryId);
  }

  private FlightInfo getInfoParallel(CallContext callContext, FlightDescriptor descriptor, String sql) {
    FlightInfo schema = getInfoImpl(callContext, descriptor, sql);
    sql = String.format("create table flight.money as (%s)", sql);
    FlightInfo ticket = getInfoImpl(callContext, descriptor, sql);
    return new FlightInfo(schema.getSchema(), schema.getDescriptor(), ticket.getEndpoints(), ticket.getBytes(), ticket.getRecords());
  }

  private FlightInfo getInfo(CallContext callContext, FlightDescriptor descriptor, Command cmd) {
    String sql = cmd.getQuery();
    if (cmd.getParallel()) {
      return getInfoParallel(callContext, descriptor, sql);
    }
    return getInfoImpl(callContext, descriptor, sql);
  }


  private FlightInfo getInfoImpl(CallContext callContext, FlightDescriptor descriptor, String sql) {
    try {
      final CreatePreparedStatementReq req =
        CreatePreparedStatementReq.newBuilder()
          .setSqlQuery(String.format(sql))
          .build();

      UserRequest request = new UserRequest(RpcType.CREATE_PREPARED_STATEMENT, req);
      Prepare prepare = new Prepare();
      submitWork(callContext, request, prepare);
      return prepare.getInfo(descriptor);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor descriptor) {
    logger.info("called get flight info");
    Command cmd = new Command();
    ProtostuffIOUtil.mergeFrom(descriptor.getCommand(), cmd, Command.getSchema());
    if (cmd.getCoalesce()) {
      return getCoalesce(callContext, descriptor, cmd);
    } else {
      return getInfo(callContext, descriptor, cmd);
    }
  }

  private UserBitShared.ExternalId submitWork(CallContext callContext, UserRequest request, UserResponseHandler handler) {
    UserBitShared.ExternalId externalId = ExternalIdHelper.generateExternalId();
    worker.get().submitWork(
      externalId,
      UserSession.Builder.newBuilder()
        .withCredentials(UserCredentials.newBuilder().setUserName(callContext.peerIdentity()).build())
        .withUserProperties(
          UserProtos.UserProperties.newBuilder().addProperties(
            UserProtos.Property.newBuilder().setKey("password").setValue(validator.password(callContext.peerIdentity()).orElse("")).build()
          ).build())
        .withOptionManager(context.get().getOptionManager()).build(),
      handler,
      request,
      TerminationListenerRegistry.NOOP);
    return externalId;
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  private class Prepare implements UserResponseHandler {

    private final CompletableFuture<CreatePreparedStatementResp> future = new CompletableFuture<>();

    public Prepare() {
    }

    public FlightInfo getInfo(FlightDescriptor descriptor) {
      try {
        CreatePreparedStatementResp handle = future.get();
        if (handle.getStatus() == RequestStatus.FAILED) {
          throw Status.UNKNOWN.withDescription(handle.getError().getMessage()).withCause(UserRemoteException.create(handle.getError())).asRuntimeException();
        }
        PreparedStatement statement = handle.getPreparedStatement();
        Ticket ticket = new Ticket(statement.getServerHandle().toByteArray());
        FlightEndpoint endpoint = new FlightEndpoint(ticket, new Location[]{location});
        FlightInfo info = new FlightInfo(fromMetadata(statement.getColumnsList()), descriptor, ImmutableList.<FlightEndpoint>of(endpoint), -1L, -1L);
        return info;
      } catch (ExecutionException e) {
        throw Throwables.propagate(e.getCause());
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    private Schema fromMetadata(List<ResultColumnMetadata> rcmd) {

      Schema schema = new Schema(rcmd.stream().map(md -> {
        ArrowType arrowType = SqlTypeNameToArrowType.toArrowType(md.getDataType());
        FieldType fieldType = new FieldType(md.getIsNullable(), arrowType, null, null);
        return new Field(md.getColumnName(), fieldType, null);
      }).collect(Collectors.toList()));
      return schema;
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      throw new IllegalStateException();
    }

    @Override
    public void completed(UserResult result) {
      if (result.getState() == QueryState.FAILED) {
        future.completeExceptionally(result.getException());
      } else {
        future.complete(result.unwrap(CreatePreparedStatementResp.class));
      }
    }

  }

  private class PrepareParallel implements UserResponseHandler {

    private final CompletableFuture<List<FlightEndpoint>> future = new CompletableFuture<>();
    private final KVStore<FlightStoreCreator.NodeKey, FlightStoreCreator.NodeKey> kvStore;
    private List<FlightEndpoint> endpoints;
    private String queryId = "unknown";


    public PrepareParallel(KVStore<FlightStoreCreator.NodeKey, FlightStoreCreator.NodeKey> kvStore) {
      this.kvStore = kvStore;
    }

    public FlightInfo getInfo(FlightDescriptor descriptor, String queryId) {
      this.queryId = queryId;
      try {
        List<FlightEndpoint> endpointsFromFuture = future.get();
        logger.debug("got endpoints future back for queryid {} with {} endpoints", queryId, endpointsFromFuture.size());
        endpoints = endpointsFromFuture.stream()
          .map(e -> {
            String ticketId = JOINER.join(queryId, new String(e.getTicket().getBytes()));
            logger.debug("doing create action for ticket {}", ticketId);
            Ticket ticket = new Ticket(ticketId.getBytes());
            try(FlightClient c = FlightClient.builder().allocator(allocator).location(e.getLocations().get(0)).build()) {
              c.authenticateBasic(SystemUser.SYSTEM_USERNAME, null);
              Iterator<Result> result = c.doAction(new Action("create", ticket.getBytes()));
              result.forEachRemaining(r -> {
                logger.debug("got action result {} for ticket {}", new String(r.getBody()), ticketId);
              });
            } catch (InterruptedException ex) {
              logger.error("unable to complete create action for ticket " + ticketId, ex);
            }
            logger.debug("completed create action for ticket {}", ticketId);
            return new FlightEndpoint(ticket, e.getLocations().toArray(new Location[0]));
          }).collect(Collectors.toList());
        FlightInfo info = new FlightInfo(new Schema(Lists.newArrayList()), descriptor, endpoints, -1L, -1L);
        logger.debug("returning new flight info with a fake schema and {} data endpoints", endpoints.size());
        return info;
      } catch (ExecutionException e) {
        throw Throwables.propagate(e.getCause());
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      logger.debug("send data is called on parallel prepare for query id {}", queryId);
      if (!future.isDone()) {
        future.complete(ImmutableList.of());
      }
      outcomeListener.success(Acks.OK, null);
    }

    @Override
    public void completed(UserResult result) {
      logger.debug("running completed method for queryid {}. Will try and run delete action now.", queryId);
      endpoints.forEach(e -> {
        String ticketId = new String(e.getTicket().getBytes());
        try(FlightClient c = FlightClient.builder().allocator(allocator).location(e.getLocations().get(0)).build()) {
          logger.debug("running delete action for {}", ticketId);
          c.authenticateBasic(SystemUser.SYSTEM_USERNAME, null);
          Iterator<Result> results = c.doAction(new Action("delete", e.getTicket().getBytes()));
          results.forEachRemaining(r -> {
            logger.debug("got action result {} for ticket {}", new String(r.getBody()), ticketId);
          });
        } catch (InterruptedException ex) {
          logger.error("delete action failed for {}", ticketId);
          throw new RuntimeException(ex);
        }
        logger.debug("successful run of delete action for {}", ticketId);
      });
      if (result.getState() == QueryState.FAILED) {
        Status.UNKNOWN.withCause(result.getException()).asRuntimeException();
      }
    }

    @Override
    public void planParallelized(PlanningSet planningSet) {
      logger.debug("plan parallel called, collecting endpoints");
      final ImmutableList.Builder<FlightEndpoint> endpoints = ImmutableList.builder();
      for (Wrapper wrapper: planningSet.getFragmentWrapperMap().values()) {
        String majorId = String.valueOf(wrapper.getMajorFragmentId());
        for (int i=0;i<wrapper.getAssignedEndpoints().size();i++) {
          CoordinationProtos.NodeEndpoint endpoint = wrapper.getAssignedEndpoint(i);

          String opName = null;
          CoreOperatorType op = CoreOperatorType.valueOf(wrapper.getNode().getRoot().getOperatorType());
          if (op == null) {
            if (wrapper.getNode().getRoot() instanceof UnionExchange) {
              opName = "UnionExchange";
            } else {
              logger.warn("unknown op type " + wrapper.getNode().getRoot());
            }
          } else {
            opName = String.valueOf(op);
          }
          Ticket ticket = new Ticket(JOINER.join(
            majorId,
            String.valueOf(i),
            opName,
            endpoint.getAddress(),
            endpoint.getUserPort()
          ).getBytes());
          FlightStoreCreator.NodeKey flightLocation = kvStore.get(FlightStoreCreator.NodeKey.fromNodeEndpoint(endpoint));
          Location location = null;
          if (flightLocation == null) {
            int port = endpoint.getUserPort();
            location = Location.forGrpcInsecure(endpoint.getAddress(), port-12104);
          } else {
            location = flightLocation.toLocation();
          }
          endpoints.add(new FlightEndpoint(ticket, location));
        }
      }
      List<FlightEndpoint> builtEndpoints = endpoints.build();
      logger.debug("built {} parallel endpoints", builtEndpoints.size());
      future.complete(builtEndpoints);
    }
  }

  private class RetrieveData implements UserResponseHandler {

    private ServerStreamListener listener;
    private RecordBatchLoader loader;
    private volatile VectorSchemaRoot root;

    public RetrieveData(ServerStreamListener listener) {
      this.listener = listener;
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      try {
        RecordBatchDef def = result.getHeader().getDef();
        if (loader == null) {
          loader = new RecordBatchLoader(allocator);
        }

        // consolidate
        try (ArrowBuf buf = allocator.buffer((int) result.getByteCount())) {
          Stream.of(result.getBuffers()).forEach(b -> {
            buf.writeBytes(b);
            b.release();
          });
          loader.load(def, buf);
        }

        if (root == null) {
          List<FieldVector> vectors = StreamSupport.stream(loader.spliterator(), false).map(v -> (FieldVector) v.getValueVector()).collect(Collectors.toList());
          root = new VectorSchemaRoot(vectors);
          listener.start(root);
        }

        root.setRowCount(result.getHeader().getRowCount());
        listener.putNext();
        outcomeListener.success(Acks.OK, null);
      } catch (Exception ex) {
        listener.error(Status.UNKNOWN.withCause(ex).withDescription(ex.getMessage()).asException());
      }
    }

    @Override
    public void completed(UserResult result) {
      if (result.getState() == QueryState.FAILED) {
        throw Status.UNKNOWN.withCause(result.getException()).asRuntimeException();
      }
      listener.completed();
    }
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener listener) {
    RetrieveData d = new RetrieveData(listener);
    RunQuery query;
    try {
      query = RunQuery.newBuilder()
        .setType(QueryType.PREPARED_STATEMENT)
        .setPreparedStatementHandle(PreparedStatementHandle.PARSER.parseFrom(ticket.getBytes()))
        .build();
    } catch (InvalidProtocolBufferException e) {
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }

    UserRequest request = new UserRequest(RpcType.RUN_QUERY, query);
    submitWork(callContext, request, d);
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> listener) {
    listener.onCompleted();
  }

  @Override
  public void listFlights(CallContext callContext, Criteria arg0, StreamListener<FlightInfo> list) {
    list.onCompleted();
  }

  @Override
  public Callable<PutResult> acceptPut(CallContext context, FlightStream flightStream) {
    throw Status.UNAVAILABLE.asRuntimeException();
  }

}
