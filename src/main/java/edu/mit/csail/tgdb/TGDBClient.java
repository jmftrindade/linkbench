package edu.mit.csail.tgdb;

import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Link;
import com.google.protobuf.Message;
import edu.mit.csail.tgdb.TGDBStoreGrpc.TGDBStoreBlockingStub;
import edu.mit.csail.tgdb.TGDBStoreGrpc.TGDBStoreStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
    // TODO: set data properties, etc.
    AddVertexRequest request =
        AddVertexRequest.newBuilder()
            .setVertex(Vertex.newBuilder().setId(node.id).build())
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
      // TODO: set data properties, etc.
      requestBuilder.addVertices(Vertex.newBuilder().setId(node.id).build());
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
    // TODO: set data properties, etc.
    AddEdgeRequest request = AddEdgeRequest.newBuilder()
                                 .setEdge(Edge.newBuilder()
                                              .setId1(link.id1)
                                              .setId2(link.id2)
                                              .setTime(link.time)
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
      // TODO: set data properties, etc.
      requestBuilder.addEdges(Edge.newBuilder()
                                  .setId1(link.id1)
                                  .setId2(link.id2)
                                  .setTime(link.time)
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
