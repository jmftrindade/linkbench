package edu.berkeley.cs;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import edu.berkeley.cs.graphstore.GraphStoreService;
import edu.berkeley.cs.graphstore.TLink;
import edu.berkeley.cs.graphstore.TNode;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

public class LinkStoreMonolog extends GraphStore {

  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private GraphStoreService.Client client;
  private TTransport transport;

  // Helper methods
  private TNode nodeToTNode(Node n) {
    return new TNode(n.id, n.type, new String(n.data));
  }

  private Node tNodeToNode(TNode n) {
    return new Node(n.id, (int) n.type, 0, 0, n.data.getBytes());
  }

  private Link tLinkToLink(TLink a) {
    return new Link(a.id1, a.link_type, a.id2, (byte) 0, a.data.getBytes(), 0, a.time);
  }

  private TLink linkToTLink(Link a) {
    return new TLink(0, a.id1, a.link_type, a.id2, a.time, new String(a.data));
  }

  /**
   * initialize the store object
   */
  @Override public void initialize(Properties p, Phase currentPhase, int threadId)
    throws Exception {
    String hostname = p.getProperty("hostname", "localhost");
    int port = Integer.parseInt(p.getProperty("port", "9090"));

    LOG.info("Attempting to connect to thrift server @ " + hostname + ":" + port);
    transport = new TSocket(hostname, port);
    client = new GraphStoreService.Client(new TBinaryProtocol(transport));
    transport.open();
    LOG.info("Connection successful.");
    if (currentPhase == Phase.LOAD) {
      addNode("", new Node(0, 0, 0, 0, new byte[0]));
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
    // Not supported
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
    return client.add_node(nodeToTNode(node));
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
    return tNodeToNode(client.get_node(type, id));
  }

  /**
   * Update all parameters of the node specified.
   *
   * @param dbid
   * @param node
   * @return true if the update was successful, false if not present
   */
  @Override public boolean updateNode(String dbid, Node node) throws Exception {
    return client.update_node(nodeToTNode(node));
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
    return client.delete_node(type, id);
  }

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override public void close() {
    transport.close();
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
    return client.add_link(linkToTLink(a));
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
    return client.delete_link(id1, link_type, id2);
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
    return client.update_link(linkToTLink(a));
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
    return tLinkToLink(client.get_link(id1, link_type, id2));
  }

  @Override public Link[] multigetLinks(String dbid, long id1, long link_type,
    long id2s[]) throws Exception {
    TreeSet<Long> id2sSet = new TreeSet<>();
    for (long id2 : id2s) {
      id2sSet.add(id2);
    }
    List<TLink> tLinkList = client.multiget_link(id1, link_type, id2sSet);
    Link[] linkList = new Link[tLinkList.size()];
    int i = 0;
    for (TLink tLink : tLinkList) {
      linkList[i++] = tLinkToLink(tLink);
    }
    return linkList;
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
    List<TLink> tLinkList = client.get_link_list(id1, link_type);
    Link[] linkList = new Link[tLinkList.size()];
    int i = 0;
    for (TLink tLink : tLinkList) {
      linkList[i++] = tLinkToLink(tLink);
    }
    return linkList;
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
    List<TLink> tLinkList =
      client.get_link_list_range(id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    Link[] linkList = new Link[tLinkList.size()];
    int i = 0;
    for (TLink tLink : tLinkList) {
      linkList[i++] = tLinkToLink(tLink);
    }
    return linkList;
  }

  @Override public long countLinks(String dbid, long id1, long link_type) throws Exception {
    return client.count_links(id1, link_type);
  }
}
