package edu.berkeley.cs;

import com.facebook.LinkBench.*;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.TransientException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.driver.v1.Values.parameters;

public class LinkStoreNeo4j extends GraphStore {
  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");

  private static Comparator<Link> linkComparator = new Comparator<Link>() {
    @Override public int compare(Link o1, Link o2) {
      if (o2.time == o1.time) {
        return 0;
      }
      return o1.time < o2.time ? 1 : -1;
    }
  };


  private static Driver driver = null;
  private Session session = null;
  private static AtomicBoolean indexCreated = new AtomicBoolean(false);

  private AtomicLong idGenerator = new AtomicLong(1L);

  // Helpers
  private static Value nodeParams(long id, int type, byte[] data) {
    return parameters("id", id, "type", type, "data", new String(data));
  }

  private static Value nodeParams(long id, int type) {
    return parameters("id", id, "type", type);
  }

  private static Value linkParams(Link l) {
    return parameters("id1", l.id1, "id2", l.id2, "time", l.time, "data", new String(l.data));
  }

  private static Value linkParams(long id1, long id2) {
    return parameters("id1", id1, "id2", id2);
  }

  private static Value linkListParams(long id1) {
    return parameters("id1", id1);
  }

  private static Value linkListParams(long id1, long minTimestamp, long maxTimestamp) {
    return parameters("id1", id1, "min_ts", minTimestamp, "max_ts", maxTimestamp);
  }

  private String linkType(long link_type) {
    return "L" + String.valueOf(link_type);
  }

  private static synchronized void initializeDriver(String server, String port) {
    if (driver == null) {
      String serverUri = "bolt://" + server + ":" + port;
      driver = GraphDatabase.driver(serverUri, AuthTokens.basic("neo4j", "neo4j"));
    }
  }

  /**
   * initialize the store object
   */
  @Override public synchronized void initialize(Properties p, Phase currentPhase, int threadId)
    throws Exception {
    LOG.info("Phase " + currentPhase.ordinal() + ", ThreadID = " + threadId + ", Object = " + this);

    String server = p.getProperty("server", "localhost");
    String port = p.getProperty("port", "7687");
    LOG.info("Server = " + server + " port = " + port);
    LinkStoreNeo4j.initializeDriver(server, port);

    session = driver.session();

    if (currentPhase == Phase.LOAD) {
      if (indexCreated.compareAndSet(false, true)) {
        LOG.info("Creating index on :Node(id)");
        try (Transaction tx = session.beginTransaction()) {
          tx.run("CREATE INDEX ON :Node(id)");
          tx.success();
          LOG.info("Index creation successful");
        }
      }
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
    session.reset();
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
    try (Transaction tx = session.beginTransaction()) {
      id = idGenerator.getAndIncrement();
      String createNodeStmt = "CREATE (n:Node {id: {id}, type: {type}, data: {data}})";
      tx.run(createNodeStmt, nodeParams(id, node.type, node.data));
      tx.success();
    } catch (TransientException e) {
      // Retry
      return addNode(dbid, node);
    }
    return id;
  }

  @Override public int bulkLoadBatchSize() {
    return 1024;
  }

  @Override public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    int i = 0;
    try (Transaction tx = session.beginTransaction()) {
      for (Node node : nodes) {
        long id = idGenerator.getAndIncrement();
        String createNodeStmt = "CREATE (n:Node {id: {id}, type: {type}, data: {data}})";
        tx.run(createNodeStmt, nodeParams(id, node.type, node.data));
        ids[i++] = id;
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      return bulkAddNodes(dbid, nodes);
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
    try (Transaction tx = session.beginTransaction()) {
      String getNodeStmt = "MATCH (n:Node {id: {id}, type: {type}}) return n.data";
      StatementResult result = tx.run(getNodeStmt, nodeParams(id, type));
      if (result.hasNext()) {
        Record record = result.next();
        byte[] data = record.get("n.data").asString().getBytes();
        tx.success();
        return new Node(id, type, 0, 0, data);
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      return getNode(dbid, type, id);
    }
    return null;
  }

  /**
   * Update all parameters of the node specified.
   *
   * @return true if the update was successful, false if not present
   */
  @Override public boolean updateNode(String dbid, Node node) throws Exception {
    boolean success;
    try (Transaction tx = session.beginTransaction()) {
      String updateNodeStmt =
        "MATCH (n:Node {id: {id}, type: {type}}) SET n.data = {data} RETURN n";
      StatementResult result = tx.run(updateNodeStmt, nodeParams(node.id, node.type, node.data));
      success = result.hasNext();
      tx.success();
    } catch (TransientException e) {
      // Retry
      return updateNode(dbid, node);
    }
    return success;
  }

  /**
   * Delete the object specified by the arguments
   *
   * @return true if the node was deleted, false if not present
   */
  @Override public boolean deleteNode(String dbid, int type, long id) throws Exception {
    int deletionCount;
    try (Transaction tx = session.beginTransaction()) {
      String deleteNodeStmt = "MATCH (n:Node {id: {id}, type: {type}}) DETACH DELETE n";
      StatementResult result = tx.run(deleteNodeStmt, nodeParams(id, type));
      deletionCount = result.consume().counters().nodesDeleted();
      tx.success();
    } catch (TransientException e) {
      // Retry
      return deleteNode(dbid, type, id);
    }
    return deletionCount > 0;
  }

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override public void close() {
    session.close();
  }

  @Override public void clearErrors(int threadID) {
    session.reset();
  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * @return true if new link added, false if updated. Implementation is
   * optional, for informational purposes only.
   * @throws Exception Arbitrary exception
   */
  @Override public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    int creationCount;
    try (Transaction tx = session.beginTransaction()) {
      String createLinkStmt =
        "MATCH (n1:Node {id: {id1}}) MATCH (n2:Node {id: {id2}}) " + "CREATE (n1)-[r:" + linkType(
          a.link_type) + " {time: {time}, data: {data}}]->(n2)";
      StatementResult result = tx.run(createLinkStmt, linkParams(a));
      creationCount = result.consume().counters().relationshipsCreated();
      tx.success();
    } catch (TransientException e) {
      // Retry
      return addLink(dbid, a, noinverse);
    } catch (ClientException e) {
      return false;
    }
    return creationCount > 0;
  }

  @Override public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
    throws Exception {
    try (Transaction tx = session.beginTransaction()) {
      for (Link a : links) {
        String createLinkStmt =
          "MATCH (n1:Node {id: {id1}}) MATCH (n2:Node {id: {id2}}) " + "CREATE (n1)-[r:" + linkType(
            a.link_type) + " {time: {time}, data: {data}}]->(n2)";
        tx.run(createLinkStmt, linkParams(a));
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      addBulkLinks(dbid, links, noinverse);
    }
  }

  @Override public void addBulkCounts(String dbid, List<LinkCount> a) throws Exception {
    // Do nothing
  }

  /**
   * Delete link identified by parameters from store
   *
   * @param expunge if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   * purposes only.
   * @throws Exception Arbitrary exception
   */
  @Override public boolean deleteLink(String dbid, long id1, long link_type, long id2,
    boolean noinverse, boolean expunge) throws Exception {
    int deletionCount;
    try (Transaction tx = session.beginTransaction()) {
      String deleteLinkStmt = "MATCH (n1:Node {id: {id1}})-[r:" + linkType(link_type)
        + "]->(n2:Node {id: {id2}}) DELETE r";
      StatementResult result = tx.run(deleteLinkStmt, linkParams(id1, id2));
      deletionCount = result.consume().counters().relationshipsDeleted();
      tx.success();
    } catch (TransientException e) {
      // Retry
      return deleteLink(dbid, id1, link_type, id2, noinverse, expunge);
    }
    return deletionCount > 0;
  }

  /**
   * Update a link in the database, or add if not found
   *
   * @return true if link found, false if new link created.  Implementation is
   * optional, for informational purposes only.
   * @throws Exception Arbitrary exception
   */
  @Override public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    boolean success;
    try (Transaction tx = session.beginTransaction()) {
      String updateLinkStmt = "MATCH (n1:Node {id: {id1}})-[r:" + linkType(a.link_type)
        + "]->(n2:Node {id: {id2}}) SET r.time = {time}, r.data = {data} RETURN r";
      StatementResult result = tx.run(updateLinkStmt, linkParams(a));
      success = result.hasNext();
      tx.success();
    } catch (TransientException e) {
      // Retry
      return updateLink(dbid, a, noinverse);
    }
    return success;
  }

  /**
   * lookup using id1, type, id2
   * Returns hidden links.
   *
   * @throws Exception Arbitrary exception
   */
  @Override public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
    try (Transaction tx = session.beginTransaction()) {
      String getLinkStmt = "MATCH (n1:Node {id: {id1}})-[r:" + linkType(link_type)
        + "]->(n2:Node {id: {id2}}) RETURN r.time, r.data";
      StatementResult result = tx.run(getLinkStmt, linkParams(id1, id2));
      if (result.hasNext()) {
        Record record = result.next();
        long time = record.get("r.time").asLong();
        byte[] data = record.get("r.data").asString().getBytes();
        tx.success();
        return new Link(id1, link_type, id2, (byte) 0, data, 0, time);
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      return getLink(dbid, id1, link_type, id2);
    }
    return null;
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * @return list of links in descending order of time, or null
   * if no matching links
   * @throws Exception Arbitrary exception
   */
  @Override public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
    ArrayList<Link> links = new ArrayList<>();
    try (Transaction tx = session.beginTransaction()) {
      String getLinkListStmt = "MATCH (n1:Node {id: {id1}})-[r:" + linkType(link_type)
        + "]->(n2:Node) RETURN r.time, r.data, n2.id";
      StatementResult result = tx.run(getLinkListStmt, linkListParams(id1));
      while (result.hasNext()) {
        Record record = result.next();
        long time = record.get("r.time").asLong();
        byte[] data = record.get("r.data").asString().getBytes();
        long id2 = record.get("n2.id").asLong();
        links.add(new Link(id1, link_type, id2, (byte) 0, data, 0, time));
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      return getLinkList(dbid, id1, link_type);
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
   * @throws Exception Arbitrary exception
   */
  @Override public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp,
    long maxTimestamp, int offset, int limit) throws Exception {
    ArrayList<Link> links = new ArrayList<>();

    try (Transaction tx = session.beginTransaction()) {
      String getLinkList2Stmt = "MATCH (n1:Node {id: {id1}})-[r:" + linkType(link_type)
        + "]->(n2:Node) WHERE r.time >= {min_ts} AND r.time <= {max_ts} RETURN r.time, r.data, n2.id";
      StatementResult result =
        tx.run(getLinkList2Stmt, linkListParams(id1, minTimestamp, maxTimestamp));
      while (result.hasNext()) {
        Record record = result.next();
        long time = record.get("r.time").asLong();
        byte[] data = record.get("r.data").asString().getBytes();
        long id2 = record.get("n2.id").asLong();
        links.add(new Link(id1, link_type, id2, (byte) 0, data, 0, time));
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      return getLinkList(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    }
    Collections.sort(links, linkComparator);
    return links.subList(offset, Math.min(links.size(), offset + limit))
      .toArray(new Link[Math.min(links.size(), limit)]);
  }

  @Override public long countLinks(String dbid, long id1, long link_type) throws Exception {
    long count = 0;
    try (Transaction tx = session.beginTransaction()) {
      String countLinksStmt =
        "MATCH (n1:Node {id: {id1}})-[r:" + linkType(link_type) + "]->(n2:Node) RETURN count(r)";
      StatementResult result = tx.run(countLinksStmt, linkListParams(id1));
      if (result.hasNext()) {
        Record record = result.next();
        count = record.get("count(r)").asLong();
      }
      tx.success();
    } catch (TransientException e) {
      // Retry
      return countLinks(dbid, id1, link_type);
    }
    return count;
  }
}
