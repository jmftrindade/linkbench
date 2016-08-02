package edu.berkeley.cs;

import com.facebook.LinkBench.*;
import com.facebook.LinkBench.Node;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.IteratorUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LinkStoreNeo4j extends GraphStore {
  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private static GraphDatabaseService db = null;
  private static Index<org.neo4j.graphdb.Node> idIndex = null;
  private static Comparator<Link> linkComparator;

  static {
    linkComparator = new Comparator<Link>() {
      @Override public int compare(Link o1, Link o2) {
        if (o2.time == o1.time) {
          return 0;
        }
        return o1.time < o2.time ? 1 : -1;
      }
    };
  }

  private AtomicLong idGenerator = new AtomicLong(1L);

  /**
   * Helper methods
   **/
  public static void registerShutdownHook(final GraphDatabaseService graphDb) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        graphDb.shutdown();
      }
    });
  }

  public RelationshipType linkTypeToRelationshipType(long linkType) {
    return DynamicRelationshipType.withName(String.valueOf(linkType));
  }

  /**
   * initialize the store object
   */
  @Override public synchronized void initialize(Properties p, Phase currentPhase, int threadId)
    throws Exception {
    LOG.info("Phase " + currentPhase.ordinal() + ", ThreadID = " + threadId + ", Object = " + this);

    if (db == null) {
      LOG.info("Initializing db...");
      String dbPath = p.getProperty("db_path", "neo4j-data");
      String pageCacheMem = p.getProperty("page_cache_size", "1g");
      boolean tuned = Boolean.valueOf(p.getProperty("tuned", "false"));
      LOG.info("Data path = " + dbPath);
      LOG.info("Tuned = " + tuned);
      LOG.info("Page Cache Memory = " + pageCacheMem);

      if (tuned) {
        LOG.info("Initializing tuned database...");
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
          .setConfig(GraphDatabaseSettings.cache_type, "none")
          .setConfig(GraphDatabaseSettings.pagecache_memory, pageCacheMem).newGraphDatabase();
        LOG.info("Completed initializing tuned database.");
      } else {
        LOG.info("Initializing untuned database...");
        db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        LOG.info("Completed initializing tuned database.");
      }
      LOG.info("Database initialization: " + db.toString());
      registerShutdownHook(db);
    }
    if (idIndex == null) {
      LOG.info("Initializing ID index...");
      try (Transaction tx = db.beginTx()) {
        idIndex = db.index().forNodes("identifier");
        tx.success();
      }
      LOG.info("Database initialization: " + idIndex.toString());
    }
    if (currentPhase == Phase.REQUEST) {
      long startId = Long.parseLong(p.getProperty("maxid1")) + 1;
      LOG.info("Request phase: setting startId to " + startId);
      idGenerator.set(startId);
    }
    LOG.info("Initialization complete.");
  }

  /**
   * Reset node storage to a clean state in shard:
   * deletes all stored nodes
   * resets id allocation, with new IDs to be allocated starting from startID
   */
  @Override public void resetNodeStore(String dbid, long startID) throws Exception {
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
  @Override public long addNode(String dbid, Node node) throws Exception {
    long id;
    try (Transaction tx = db.beginTx()) {
      id = idGenerator.getAndIncrement();
      org.neo4j.graphdb.Node neoNode = db.createNode();
      neoNode.setProperty("id", id);
      neoNode.setProperty("data", node.data);
      idIndex.add(neoNode, "id", id);
      tx.success();
    }
    return id;
  }

  @Override public int bulkLoadBatchSize() {
    return 1024;
  }

  @Override public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    int i = 0;
    try (Transaction tx = db.beginTx()) {
      for (Node node : nodes) {
        long id = idGenerator.getAndIncrement();
        org.neo4j.graphdb.Node neoNode = db.createNode();
        neoNode.setProperty("id", id);
        neoNode.setProperty("data", node.data);
        idIndex.add(neoNode, "id", id);
        ids[i++] = id;
      }
      tx.success();
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
  @Override public Node getNode(String dbid, int type, long id) throws Exception {
    org.neo4j.graphdb.Node neoNode;
    try (Transaction tx = db.beginTx()) {
      neoNode = IteratorUtil.firstOrNull(idIndex.get("id", id).iterator());
      if (neoNode != null) {
        tx.success();
        return new Node(id, 0, 0, 0, (byte[]) neoNode.getProperty("data"));
      }
      tx.success();
    }
    return null;
  }

  /**
   * Update all parameters of the node specified.
   *
   * @return true if the update was successful, false if not present
   */
  @Override public boolean updateNode(String dbid, Node node) throws Exception {
    try (Transaction tx = db.beginTx()) {
      org.neo4j.graphdb.Node neoNode = IteratorUtil.firstOrNull(idIndex.get("id", node.id).iterator());
      if (neoNode == null) {
        tx.failure();
        return false;
      }
      neoNode.setProperty("id", node.id);
      neoNode.setProperty("data", node.data);
      idIndex.add(neoNode, "id", node.id);
      tx.success();
    }
    return true;
  }

  /**
   * Delete the object specified by the arguments
   *
   * @return true if the node was deleted, false if not present
   */
  @Override public boolean deleteNode(String dbid, int type, long id) throws Exception {
    try (Transaction tx = db.beginTx()) {

      org.neo4j.graphdb.Node neoNode = IteratorUtil.firstOrNull(idIndex.get("id", id).iterator());
      if (neoNode == null) {
        tx.failure();
        return false;
      }
      for (Relationship relationship : neoNode.getRelationships()) {
        relationship.delete();
      }
      idIndex.remove(neoNode);
      neoNode.delete();
      tx.success();
    }
    return true;
  }

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override public void close() {
  }

  @Override public void clearErrors(int threadID) {
    // Do nothing
  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * @return true if new link added, false if updated. Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", a.id1).iterator());
      final org.neo4j.graphdb.Node dst = IteratorUtil.firstOrNull(idIndex.get("id", a.id2).iterator());
      if (src == null || dst == null) {
        tx.failure();
        return false;
      }
      Relationship rel = src.createRelationshipTo(dst, linkTypeToRelationshipType(a.link_type));
      rel.setProperty("time", a.time);
      rel.setProperty("data", a.data);
      tx.success();
    }
    return true;
  }

  @Override public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
    throws Exception {
    try (Transaction tx = db.beginTx()) {
      for (Link a : links) {
        final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", a.id1).iterator());
        final org.neo4j.graphdb.Node dst = IteratorUtil.firstOrNull(idIndex.get("id", a.id2).iterator());
        if (src == null || dst == null) {
          tx.failure();
          return;
        }
        Relationship rel = src.createRelationshipTo(dst, linkTypeToRelationshipType(a.link_type));
        rel.setProperty("time", a.time);
        rel.setProperty("data", a.data);
      }
      tx.success();
    }
  }

  @Override public void addBulkCounts(String dbid, List<LinkCount> a) throws Exception {
    // Do nothing.
  }

  /**
   * Delete link identified by parameters from store
   *
   * @param expunge if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   * purposes only.
   * @throws Exception
   */
  @Override public boolean deleteLink(String dbid, long id1, long link_type, long id2,
    boolean noinverse, boolean expunge) throws Exception {
    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", id1).iterator());
      if (src == null) {
        tx.failure();
        return false;
      }
      Iterable<Relationship> rels =
        src.getRelationships(linkTypeToRelationshipType(link_type), Direction.OUTGOING);
      for (Relationship rel : rels) {
        if ((long) rel.getEndNode().getProperty("id") == id2) {
          rel.delete();
          tx.success();
          return true;
        }
      }
      tx.success();
    }
    return false;
  }

  /**
   * Update a link in the database, or add if not found
   *
   * @return true if link found, false if new link created.  Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", a.id1).iterator());
      final org.neo4j.graphdb.Node dst = IteratorUtil.firstOrNull(idIndex.get("id", a.id2).iterator());
      if (src == null || dst == null) {
        tx.failure();
        return false;
      }
      Iterable<Relationship> rels =
        src.getRelationships(linkTypeToRelationshipType(a.link_type), Direction.OUTGOING);
      for (Relationship rel : rels) {
        if ((long) rel.getEndNode().getProperty("id") == a.id2) {
          rel.setProperty("time", a.time);
          rel.setProperty("data", a.data);
          tx.success();
          return true;
        }
      }
      tx.failure();
    }
    return true;
  }

  /**
   * lookup using id1, type, id2
   * Returns hidden links.
   *
   * @throws Exception
   */
  @Override public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", id1).iterator());
      if (src == null) {
        tx.failure();
        return null;
      }
      Iterable<Relationship> rels =
        src.getRelationships(linkTypeToRelationshipType(link_type), Direction.OUTGOING);

      for (Relationship rel : rels) {
        if ((long) rel.getEndNode().getProperty("id") == id2) {
          return new Link(id1, link_type, id2, (byte) 0, (byte[]) rel.getProperty("data"), 0,
            (long) rel.getProperty("time"));
        }
      }
      tx.success();
    }
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
  @Override public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
    ArrayList<Link> links = new ArrayList<>();
    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", id1).iterator());
      if (src == null) {
        tx.failure();
        return null;
      }
      Iterable<Relationship> rels =
        src.getRelationships(linkTypeToRelationshipType(link_type), Direction.OUTGOING);
      for (Relationship rel : rels) {
        links.add(new Link(id1, link_type, (long) rel.getEndNode().getProperty("id"), (byte) 0,
          (byte[]) rel.getProperty("data"), 0, (long) rel.getProperty("time")));
      }
      tx.success();
    }
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
  @Override public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp,
    long maxTimestamp, int offset, int limit) throws Exception {
    ArrayList<Link> links = new ArrayList<>();

    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", id1).iterator());
      if (src == null) {
        tx.failure();
        return null;
      }
      Iterable<Relationship> rels =
        src.getRelationships(linkTypeToRelationshipType(link_type), Direction.OUTGOING);
      byte unused = 0;
      for (Relationship rel : rels) {
        long id2 = (long) rel.getEndNode().getProperty("id");
        long time = (long) rel.getProperty("time");
        if (time >= minTimestamp && time <= maxTimestamp) {
          links.add(new Link(id1, link_type, id2, unused, (byte[]) rel.getProperty("data"), 0, time));
        }
      }
      tx.success();
    }

    Collections.sort(links, linkComparator);
    return links.subList(offset, Math.min(links.size(), offset + limit))
      .toArray(new Link[Math.min(links.size(), limit)]);
  }

  @Override public long countLinks(String dbid, long id1, long link_type) throws Exception {
    long count = 0;
    try (Transaction tx = db.beginTx()) {
      final org.neo4j.graphdb.Node src = IteratorUtil.firstOrNull(idIndex.get("id", id1).iterator());
      if (src == null) {
        tx.failure();
        return 0;
      }
      Iterable<Relationship> rels =
        src.getRelationships(linkTypeToRelationshipType(link_type), Direction.OUTGOING);
      for (Relationship ignored : rels) {
        count++;
      }
      tx.success();
    }
    return count;
  }
}
