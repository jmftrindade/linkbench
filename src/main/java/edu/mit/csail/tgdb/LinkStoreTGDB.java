package edu.mit.csail.tgdb;

import com.facebook.LinkBench.*;
import com.facebook.LinkBench.Node;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LinkStoreTGDB extends GraphStore {
  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private static TGDBClient dbClient = null;
  private static Comparator<Link> linkComparator;

  static {
    linkComparator = new Comparator<Link>() {
      @Override
      public int compare(Link o1, Link o2) {
        if (o2.time == o1.time) {
          return 0;
        }
        return o1.time < o2.time ? 1 : -1;
      }
    };
  }

  private AtomicLong idGenerator = new AtomicLong(1L);

  /**
   * initialize the store object
   */
  @Override
  public synchronized void initialize(Properties p, Phase currentPhase,
                                      int threadId) throws Exception {
    if (dbClient != null) {
      LOG.warn("TGDB already initialized. Returning.");
      return;
    }

    LOG.info("Phase " + currentPhase.ordinal() + ", ThreadID = " + threadId +
             ", Object = " + this);
    LOG.info("Initializing TGDB...");

    dbClient = new TGDBClient("localhost", 50051);
    try {
      dbClient.initialize();
    } finally {
      // FIXME: remove shutdown from here.
      dbClient.shutdown();
    }

    LOG.info("Initialization complete.");
  }

  /**
   * Reset node storage to a clean state in shard:
   * deletes all stored nodes
   * resets id allocation, with new IDs to be allocated starting from startID
   */
  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    idGenerator.set(startID);
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
  @Override
  public long addNode(String dbid, Node node) throws Exception {
    long id;

    // Use this as node id for the node being added.
    id = idGenerator.getAndIncrement();
    LOG.info("node.id = " + node.id + ", gen'ed id=" + id);
    node.id = id;

    dbClient.addNode(node);

    return id;
  }

  @Override
  public int bulkLoadBatchSize() {
    // TODO: another value?

    return 1024;
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];

    int i = 0;
    for (Node node : nodes) {
      long id = idGenerator.getAndIncrement();
      ids[i++] = id;
      // TODO
    }

    return ids;
  }

  /**
   * Get a node of the specified type
   *
   * @param dbid the db shard the id is mapped to
   * @param type the type of the object
   * @param id   the id of the object
   * @return null if not found, a Node with all fields filled in otherwise
   */
  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    // TODO

    return null;
  }

  /**
   * Update all parameters of the node specified.
   *
   * @return true if the update was successful, false if not present
   */
  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {
    // TODO

    return true;
  }

  /**
   * Delete the object specified by the arguments
   *
   * @return true if the node was deleted, false if not present
   */
  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    // TODO

    return true;
  }

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override
  public void close() {
    // TODO
  }

  @Override
  public void clearErrors(int threadID) {
    // Do nothing
  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * @return true if new link added, false if updated. Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override
  public boolean addLink(String dbid, Link a, boolean noinverse)
      throws Exception {
    return true;
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
      throws Exception {
    // TODO
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> a) throws Exception {
    // TODO
  }

  /**
   * Delete link identified by parameters from store
   *
   * @param expunge if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   * purposes only.
   * @throws Exception
   */
  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                            boolean noinverse, boolean expunge)
      throws Exception {
    return true;
  }

  /**
   * Update a link in the database, or add if not found
   *
   * @return true if link found, false if new link created.  Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override
  public boolean updateLink(String dbid, Link a, boolean noinverse)
      throws Exception {
    return true;
  }

  /**
   * lookup using id1, type, id2
   * Returns hidden links.
   *
   * @throws Exception
   */
  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
      throws Exception {
    return null;
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception
   */
  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
      throws Exception {
    ArrayList<Link> links = new ArrayList<>();

    // TODO: actually retrieve links

    Collections.sort(links, linkComparator);
    return links.toArray(new Link[links.size()]);
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception
   */
  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp, int offset,
                            int limit) throws Exception {
    ArrayList<Link> links = new ArrayList<>();

    // TODO actually fetch links

    Collections.sort(links, linkComparator);
    return links.subList(offset, Math.min(links.size(), offset + limit))
        .toArray(new Link[Math.min(links.size(), limit)]);
  }

  @Override
  public long countLinks(String dbid, long id1, long link_type)
      throws Exception {
    long count = 0;

    // TODO: actually fetch links and count

    return count;
  }
}
