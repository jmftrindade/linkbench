package edu.mit.csail.tgdb;

import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkStore;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import edu.mit.csail.tgdb.TGDBStoreGrpc.TGDBStoreBlockingStub;
import edu.mit.csail.tgdb.TGDBStoreGrpc.TGDBStoreStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TGDBClient {
  private static final Logger logger =
      Logger.getLogger(TGDBClient.class.getName());

  private final ManagedChannel channel;
  private final TGDBStoreBlockingStub blockingStub;
  private final TGDBStoreStub asyncStub;

  /** Construct client for TGDB server at host:port  */
  public TGDBClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  /** Construct client using the existing channel */
  public TGDBClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = TGDBStoreGrpc.newBlockingStub(channel);
    asyncStub = TGDBStoreGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** ======= Service API ====== */
  public void addNode(Node node) {
    // Set data properties.
    Struct.Builder struct = Struct.newBuilder();
    struct.putFields(
        "data",
        Value.newBuilder().setStringValue(Arrays.toString(node.data)).build());
    struct.putFields("version",
                     Value.newBuilder().setNumberValue(node.version).build());

    AddVertexRequest request = AddVertexRequest.newBuilder()
                                   .setVertex(Vertex.newBuilder()
                                                  .setId(node.id)
                                                  .setType(node.type)
                                                  .setTime(node.time)
                                                  .setProperties(struct.build())
                                                  .build())
                                   .build();
    AddVertexResponse response;

    try {
      response = blockingStub.addVertex(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }
  }

  public void addNodes(List<Node> nodes) {
    AddVerticesRequest.Builder requestBuilder = AddVerticesRequest.newBuilder();
    for (Node node : nodes) {
      // Set data properties.
      Struct.Builder struct = Struct.newBuilder();
      struct.putFields("data", Value.newBuilder()
                                   .setStringValue(Arrays.toString(node.data))
                                   .build());
      struct.putFields("version",
                       Value.newBuilder().setNumberValue(node.version).build());

      requestBuilder.addVertices(Vertex.newBuilder()
                                     .setId(node.id)
                                     .setType(node.type)
                                     .setTime(node.time)
                                     .setProperties(struct.build())
                                     .build());
    }
    AddVerticesRequest request = requestBuilder.build();
    AddVerticesResponse response;

    try {
      response = blockingStub.addVertices(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }
  }

  public void addLink(Link link) {
    Struct.Builder struct = Struct.newBuilder();
    struct.putFields(
        "data",
        Value.newBuilder().setStringValue(Arrays.toString(link.data)).build());
    struct.putFields("version",
                     Value.newBuilder().setNumberValue(link.version).build());
    // XXX: Set +INF as end_time?
    AddEdgeRequest request = AddEdgeRequest.newBuilder()
                                 .setEdge(Edge.newBuilder()
                                              .setSrcId(link.id1)
                                              .setType(link.link_type)
                                              .setDstId(link.id2)
                                              .setStartTime(link.time)
                                              .setProperties(struct.build())
                                              .build())
                                 .build();
    AddEdgeResponse response;

    try {
      response = blockingStub.addEdge(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }
  }

  public void addLinks(List<Link> links) {
    AddEdgesRequest.Builder requestBuilder = AddEdgesRequest.newBuilder();
    for (Link link : links) {
      // Set data properties.
      Struct.Builder struct = Struct.newBuilder();
      struct.putFields("data", Value.newBuilder()
                                   .setStringValue(Arrays.toString(link.data))
                                   .build());
      struct.putFields("version",
                       Value.newBuilder()
                           .setNumberValue((double)link.version * 1.0)
                           .build());
      // XXX: Set +INF as end_time?
      requestBuilder.addEdges(Edge.newBuilder()
                                  .setSrcId(link.id1)
                                  .setType(link.link_type)
                                  .setDstId(link.id2)
                                  .setStartTime(link.time)
                                  .setProperties(struct.build())
                                  .build());
    }
    AddEdgesRequest request = requestBuilder.build();
    AddEdgesResponse response;

    try {
      response = blockingStub.addEdges(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }
  }

  public Link getLink(long id1, long link_type, long id2) {
    GetEdgeRequest.Builder requestBuilder = GetEdgeRequest.newBuilder();
    requestBuilder.setSrcId(id1).setType(link_type).setDstId(id2);
    GetEdgeRequest request = requestBuilder.build();
    GetEdgeResponse response = null;

    try {
      response = blockingStub.getEdge(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }

    if (response == null || !response.hasEdge()) {
      return null;
    }

    edu.mit.csail.tgdb.Edge e = response.getEdge();
    byte[] data = null;

    // XXX: linkbench defines version as long for nodes, and as int for links.
    int version = 0;
    if (e.hasProperties()) {
      Map<String, Value> fields = e.getProperties().getFields();
      String d = new String(fields.get("data").getStringValue());
      data = d.getBytes();

      // XXX: linkbench defines version as long for nodes, and as int for links.
      version = (int)fields.get("version").getNumberValue();
    }
    return new Link(e.getSrcId(), e.getType(), e.getDstId(),
                    LinkStore.VISIBILITY_DEFAULT, data, version,
                    e.getStartTime());
  }

  public Node getNode(int type, long id) {
    GetVertexRequest.Builder requestBuilder = GetVertexRequest.newBuilder();
    requestBuilder.setId(id).setType(type);
    GetVertexRequest request = requestBuilder.build();
    GetVertexResponse response = null;

    try {
      response = blockingStub.getVertex(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }

    if (response == null || !response.hasVertex()) {
      return null;
    }

    Vertex v = response.getVertex();
    byte[] data = null;

    // XXX: linkbench defines version as long for nodes, and as int for links.
    long version = 0;
    if (v.hasProperties()) {
      Map<String, Value> fields = v.getProperties().getFields();
      String d = new String(fields.get("data").getStringValue());
      data = d.getBytes();

      // XXX: linkbench defines version as long for nodes, and as int for links.
      version = (long)fields.get("version").getNumberValue();
    }

    return new Node(v.getId(), v.getType(), version, v.getTime(), data);
  }

  public void initialize() {
    InitializeRequest request = InitializeRequest.newBuilder().build();
    InitializeResponse response;

    try {
      response = blockingStub.initialize(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }

    return;
  }
}
