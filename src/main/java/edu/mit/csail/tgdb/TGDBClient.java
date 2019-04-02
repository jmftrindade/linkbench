package edu.mit.csail.tgdb;

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
  private final TGDBLinkStoreBlockingStub blockingStub;
  private final TGDBLinkStoreStub asyncStub;

  /** Construct client for TGDB server at host:port  */
  public TGDBClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  /** Construct client using the existing channel */
  public TGDBClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = TGDBLinkStoreGrpc.newBlockingStub(channel);
    asyncStub = TGDBLinkStoreGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** ======= Service API ====== */
  public void addNode(Node node) {
    AddVertexRequest request =
        AddVertexRequest.newBuilder()
            .setVertex(Vertex.newBuilder().setId(node.id))
            .build();
    AddEdgeResponse response;

    try {
      response = blockingStub.addEdge(request);
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
