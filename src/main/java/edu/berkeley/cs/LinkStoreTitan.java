package edu.berkeley.cs;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LinkStoreTitan extends GraphStore {
  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private static TitanGraph g = null;
  private AtomicLong idGenerator = new AtomicLong(1L);
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

  /**
   * initialize the store object
   */
  @Override public synchronized void initialize(Properties p, Phase currentPhase, int threadId)
    throws IOException {
    LOG.info("Phase " + currentPhase.ordinal() + ", ThreadID = " + threadId + ", Object = " + this);
    if (g == null && currentPhase == Phase.LOAD) {
      String configFile = p.getProperty("titan.config_file");
      String keyspace = p.getProperty("titan.cassandra.keyspace");
      LOG.info("Reading from configuration file " + configFile);
      Configuration conf = null;
      try {
        conf = new PropertiesConfiguration(configFile);
        conf.setProperty("titan.cassandra.keyspace", keyspace);
      } catch (ConfigurationException e) {
        LOG.info("Error reading configuration: " + e.getMessage());
      }
      LOG.info("Creating connection to Titan...");
      try {
        g = TitanFactory.open(conf);
      } catch (Exception e) {
        LOG.info("Error connecting to Titan: " + e.getMessage());
        throw e;
      }
      LOG.info("Connection successful.");
    }
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
    TitanTransaction tx = g.buildTransaction().start();
    Vertex v = tx.addVertex(null);
    long id = idGenerator.getAndIncrement();
    v.setProperty("iid", id);
    v.setProperty("data", node.data);
    tx.commit();
    return id;
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
    Vertex v;
    try {
      v = g.getVertices("iid", id).iterator().next();
    } catch (NoSuchElementException e) {
      return null;
    }
    return new Node(id, 0, 0, 0, (byte[]) v.getProperty("data"));
  }

  /**
   * Update all parameters of the node specified.
   *
   * @return true if the update was successful, false if not present
   */
  @Override public boolean updateNode(String dbid, Node node) throws Exception {
    TitanTransaction tx = g.buildTransaction().start();
    Vertex v;
    try {
      v = tx.getVertices("iid", node.id).iterator().next();
    } catch (NoSuchElementException e) {
      return false;
    }
    v.setProperty("data", node.data);
    tx.commit();
    return true;
  }

  /**
   * Delete the object specified by the arguments
   *
   * @return true if the node was deleted, false if not present
   */
  @Override public boolean deleteNode(String dbid, int type, long id) throws Exception {
    TitanTransaction tx = g.buildTransaction().start();
    Vertex v;
    try {
      v = tx.getVertices("iid", id).iterator().next();
    } catch (NoSuchElementException e) {
      tx.rollback();
      return false;
    }

    for (Edge e : v.getEdges(Direction.OUT)) {
      e.remove();
    }
    v.remove();
    tx.commit();
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
    TitanTransaction tx = g.buildTransaction().start();
    Vertex src, dst;
    try {
      src = tx.getVertices("iid", a.id1).iterator().next();
      dst = tx.getVertices("iid", a.id2).iterator().next();
    } catch (NoSuchElementException e) {
      tx.rollback();
      return false;
    }
    Edge e = tx.addEdge(null, src, dst, String.valueOf(a.link_type));
    e.setProperty("time", a.time);
    e.setProperty("data", a.data);
    tx.commit();
    return true;
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
    TitanTransaction tx = g.buildTransaction().start();
    Vertex src;
    try {
      src = tx.getVertices("iid", id1).iterator().next();
    } catch (NoSuchElementException e) {
      tx.rollback();
      return false;
    }
    Iterable<Edge> edges = src.getEdges(Direction.OUT);
    for (Edge edge : edges) {
      if ((long) edge.getVertex(Direction.IN).getProperty("iid") == id2
        && edge.getLabel().compareToIgnoreCase(String.valueOf(link_type)) == 0) {
        edge.remove();
        tx.commit();
        return true;
      }
    }
    tx.commit();
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
    TitanTransaction tx = g.buildTransaction().start();
    Vertex src;
    try {
      src = tx.getVertices("iid", a.id1).iterator().next();
    } catch (NoSuchElementException e) {
      tx.rollback();
      return false;
    }
    Iterable<Edge> edges = src.getEdges(Direction.OUT);
    for (Edge edge : edges) {
      if ((long) edge.getVertex(Direction.IN).getProperty("iid") == a.id2
        && edge.getLabel().compareToIgnoreCase(String.valueOf(a.link_type)) == 0) {
        edge.setProperty("time", a.time);
        edge.setProperty("data", a.data);
        tx.commit();
        return true;
      }
    }
    tx.commit();
    return false;
  }

  /**
   * lookup using id1, type, id2
   * Returns hidden links.
   *
   * @throws Exception
   */
  @Override public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
    Vertex src;
    try {
      src = g.getVertices("iid", id1).iterator().next();
    } catch (NoSuchElementException e) {
      return null;
    }
    Iterable<Edge> edges = src.getEdges(Direction.OUT);
    for (Edge edge : edges) {
      if ((long) edge.getVertex(Direction.IN).getProperty("iid") == id2
        && edge.getLabel().compareToIgnoreCase(String.valueOf(link_type)) == 0) {
        return new Link(id1, link_type, id2, (byte) 0, (byte[]) edge.getProperty("data"), 0,
          (long) edge.getProperty("time"));
      }
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
    Vertex src;
    try {
      src = g.getVertices("iid", id1).iterator().next();
    } catch (NoSuchElementException e) {
      return null;
    }
    Iterable<Edge> edges = src.getEdges(Direction.OUT);
    ArrayList<Link> links = new ArrayList<>();
    for (Edge edge : edges) {
      if (edge.getLabel().compareToIgnoreCase(String.valueOf(link_type)) == 0) {
        Vertex dst = edge.getVertex(Direction.IN);
        links.add(new Link(id1, link_type, (Long) dst.getProperty("iid"), (byte) 0,
          (byte[]) edge.getProperty("data"), 0, (long) edge.getProperty("time")));
      }
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
    Vertex src;
    try {
      src = g.getVertices("iid", id1).iterator().next();
    } catch (NoSuchElementException e) {
      return null;
    }
    Iterable<Edge> edges = src.getEdges(Direction.OUT);
    ArrayList<Link> links = new ArrayList<>();
    for (Edge edge : edges) {
      Vertex dst = edge.getVertex(Direction.IN);
      long id2 = dst.getProperty("iid");
      long time = edge.getProperty("time");
      if (time >= minTimestamp && time >= maxTimestamp &&
        edge.getLabel().compareToIgnoreCase(String.valueOf(link_type)) == 0) {
        byte[] data = edge.getProperty("data");
        links.add(new Link(id1, link_type, id2, (byte) 0, data, 0, time));
      }
    }
    Collections.sort(links, linkComparator);
    return links.subList(offset, Math.min(links.size(), offset + limit))
      .toArray(new Link[Math.min(links.size(), limit)]);
  }

  @Override public long countLinks(String dbid, long id1, long link_type) throws Exception {
    Vertex src;
    try {
      src = g.getVertices("iid", id1).iterator().next();
    } catch (NoSuchElementException e) {
      return 0;
    }
    long count = 0;
    Iterable<Edge> edges = src.getEdges(Direction.OUT);
    for (Edge edge : edges) {
      if (edge.getLabel().compareToIgnoreCase(String.valueOf(link_type)) == 0) {
        count++;
      }
    }
    return count;
  }
}
