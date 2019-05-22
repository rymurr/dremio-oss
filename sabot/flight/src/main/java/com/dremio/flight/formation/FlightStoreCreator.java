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
package com.dremio.flight.formation;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.exec.proto.CoordinationProtos;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;

import java.io.IOException;

public final class
FlightStoreCreator implements StoreCreationFunction<KVStore<FlightStoreCreator.NodeKey, FlightStoreCreator.NodeKey>> {
  @Override
  public KVStore<NodeKey, NodeKey> build(StoreBuildingFactory factory) {
    return factory.<NodeKey, NodeKey>newStore()
      .name("FLIGHT_NODES")
      .keySerializer(NodeKeySerializer.class)
      .valueSerializer(NodeKeySerializer.class)
      .build();
  }

  public static final class NodeKey {
    private String host;
    private int port;
    private String name;

    public NodeKey() {
    }

    public NodeKey(String host, int port, String name) {
      this.host = host;
      this.port = port;
      this.name = name;
    }

    public static NodeKey fromNodeEndpoint(CoordinationProtos.NodeEndpoint n) {
      return new NodeKey(n.getAddress(), n.getUserPort(), "");
    }

    public static NodeKey fromFlightEndpoint(Location location) {
      return new NodeKey(location.getUri().getHost(), location.getUri().getPort(), "");
    }

    public Location toLocation() {
      return Location.forGrpcInsecure(host, port);
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return "NodeKey{" +
        "host='" + host + '\'' +
        ", port=" + port +
        ", name='" + name + '\'' +
        '}';
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NodeKey nodeKey = (NodeKey) o;
      return port == nodeKey.port &&
        Objects.equal(host, nodeKey.host) &&
        Objects.equal(name, nodeKey.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(host, port, name);
    }
  }

  public static final class NodeKeySerializer extends Serializer<NodeKey> {
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public String toJson(NodeKey v) throws IOException {
      return mapper.writeValueAsString(v);
    }

    @Override
    public NodeKey fromJson(String v) throws IOException {
      return mapper.readValue(v, NodeKey.class);
    }

    @Override
    public byte[] convert(NodeKey v) {
      try {
        return mapper.writeValueAsBytes(v);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public NodeKey revert(byte[] v) {
      try {
        return mapper.readValue(v, NodeKey.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
