package edu.berkeley.cs;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class DataGen extends GraphStore {

  class NodeStore {
    private BufferedWriter writer;
    private AtomicLong currentId;

    public NodeStore(String path, long startId) throws IOException {
      this.writer = new BufferedWriter(new FileWriter(path));
      this.currentId = new AtomicLong(startId);
    }

    public long addNode(Node node) throws IOException {
      long id = currentId.getAndIncrement();
      writer.write(new String(node.data) + "\n");
      return id;
    }

    public void close() throws IOException {
      writer.close();
    }
  }

  class LinkStore {
    private BufferedWriter writer;

    public LinkStore(String path) throws IOException {
      this.writer = new BufferedWriter(new FileWriter(path));
    }

    private boolean addLink(Link link) throws IOException {
      writer.write(String.valueOf(link.id1));
      writer.write(" ");
      writer.write(String.valueOf(link.id2));
      writer.write(" ");
      writer.write(String.valueOf(link.link_type));
      writer.write(" ");
      writer.write(String.valueOf(link.time));
      writer.write(" ");
      writer.write(new String(link.data));
      writer.write("\n");
      return true;
    }

    public void close() throws IOException {
      writer.close();
    }
  }

  private String dataPath;
  private HashMap<String, NodeStore> nodeStores;
  private HashMap<String, LinkStore> linkStores;

  public DataGen() {
    this.dataPath = "data";
    this.nodeStores = new HashMap<>();
    this.linkStores = new HashMap<>();
  }

  /**
   * initialize the store object
   *
   * @param p
   * @param currentPhase
   * @param threadId
   */
  @Override public void initialize(Properties p, Phase currentPhase, int threadId)
    throws IOException, IOException {
    // Do nothing
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
    synchronized (nodeStores) {
      NodeStore store = nodeStores.get(dbid);
      if (store != null)
        store.close();
      nodeStores.put(dbid, new NodeStore(dataPath + "_" + dbid + ".node", startID));
    }
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
    synchronized (nodeStores) {
      NodeStore store = nodeStores.get(dbid);
      return store.addNode(node);
    }
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

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override public void close() {
    for (NodeStore store : nodeStores.values()) {
      try {
        store.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    for (LinkStore store: linkStores.values()) {
      try {
        store.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override public void clearErrors(int threadID) {
    // Do nothing
  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * @param dbid
   * @param a
   * @param noinverse
   * @return true if new link added, false if updated. Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    synchronized (linkStores) {
      LinkStore store = linkStores.get(dbid);
      if (store == null) {
        store = new LinkStore(dataPath + "_" + dbid + ".assoc");
        linkStores.put(dbid, store);
      }
      return store.addLink(a);
    }

  }

  /**
   * Delete link identified by parameters from store
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param id2
   * @param noinverse
   * @param expunge   if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   * purposes only.
   * @throws Exception
   */
  @Override public boolean deleteLink(String dbid, long id1, long link_type, long id2,
    boolean noinverse, boolean expunge) throws Exception {
    return false;
  }

  /**
   * Update a link in the database, or add if not found
   *
   * @param dbid
   * @param a
   * @param noinverse
   * @return true if link found, false if new link created.  Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    return false;
  }

  /**
   * lookup using id1, type, id2
   * Returns hidden links.
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param id2
   * @return
   * @throws Exception
   */
  @Override public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
    return null;
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception
   */
  @Override public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
    return new Link[0];
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param minTimestamp
   * @param maxTimestamp
   * @param offset
   * @param limit
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception
   */
  @Override public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp,
    long maxTimestamp, int offset, int limit) throws Exception {
    return new Link[0];
  }

  @Override public long countLinks(String dbid, long id1, long link_type) throws Exception {
    return 0;
  }
}
