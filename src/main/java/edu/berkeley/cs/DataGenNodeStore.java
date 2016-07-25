package edu.berkeley.cs;

import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.NodeStore;
import com.facebook.LinkBench.Phase;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class DataGenNodeStore implements NodeStore {
  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private BufferedWriter writer = null;
  private AtomicLong currentId = new AtomicLong(1L);

  /**
   * initialize the store object
   *
   * @param p
   * @param currentPhase
   * @param threadId
   */
  @Override public void initialize(Properties p, Phase currentPhase, int threadId)
    throws Exception {
    if (writer == null && currentPhase == Phase.LOAD) {
      this.writer = new BufferedWriter(new FileWriter("data.node"));
    }
  }

  /**
   * Reset node storage to a clean state in shard:
   * deletes all stored nodes
   * resets id allocation, with new IDs to be allocated starting from startID
   *
   * @param dbid
   * @param startID
   */
  @Override public void resetNodeStore(String dbid, long startID) throws Exception {
    this.currentId.set(startID);
  }

  /**
   * Adds a new node object to the database.
   * <p/>
   * This allocates a new id for the object and returns i.
   * <p/>
   * The benchmark assumes that, after resetStore() is called,
   * node IDs are allocated in sequence, i.e. startID, startID + 1, ...
   * Add node should return the next ID in the sequence.
   *
   * @param dbid the db shard to put that object in
   * @param node a node with all data aside from id filled in.  The id
   *             field is *not* updated to the new value by this function
   * @return the id allocated for the node
   */
  @Override public long addNode(String dbid, Node node) throws Exception {
    long id = currentId.getAndIncrement();
    writer.write(new String(node.data) + "\n");
    return id;
  }

  /**
   * Bulk loading to more efficiently load nodes.
   * Calling this is equivalent to calling addNode multiple times.
   *
   * @param dbid
   * @param nodes
   * @return the actual IDs allocated to the nodes
   * @throws Exception
   */
  @Override public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    return new long[0];
  }

  /**
   * Preferred size of data to load
   *
   * @return
   */
  @Override public int bulkLoadBatchSize() {
    return 0;
  }

  /**
   * Get a node of the specified type
   *
   * @param dbid the db shard the id is mapped to
   * @param type the type of the object
   * @param id   the id of the object
   * @return null if not found, a Node with all fields filled in otherwise
   */
  @Override public Node getNode(String dbid, int type, long id) throws Exception {
    return null;
  }

  /**
   * Update all parameters of the node specified.
   *
   * @param dbid
   * @param node
   * @return true if the update was successful, false if not present
   */
  @Override public boolean updateNode(String dbid, Node node) throws Exception {
    return false;
  }

  /**
   * Delete the object specified by the arguments
   *
   * @param dbid
   * @param type
   * @param id
   * @return true if the node was deleted, false if not present
   */
  @Override public boolean deleteNode(String dbid, int type, long id) throws Exception {
    return false;
  }

  @Override public void clearErrors(int loaderId) {

  }

  /**
   * Close the node store and clean up any resources
   */
  @Override public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      LOG.error("Could not close node writer: " + e.getMessage());
    }
  }
}
