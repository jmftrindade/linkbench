package edu.berkeley.cs;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.Phase;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class DataGenLinkStore extends LinkStore {
  private final Logger LOG = Logger.getLogger("com.facebook.linkbench");
  private BufferedWriter writer = null;

  /**
   * initialize the store object
   *
   * @param p
   * @param currentPhase
   * @param threadId
   */
  @Override public synchronized void initialize(Properties p, Phase currentPhase, int threadId)
    throws Exception {
    if (writer == null) {
      this.writer = new BufferedWriter(new FileWriter("link.stats"));
    }
  }

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  @Override public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      LOG.error("Could not close link writer: " + e.getMessage());
    }
  }

  @Override public void clearErrors(int threadID) {

  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * @param dbid
   * @param link
   * @param noinverse
   * @return true if new link added, false if updated. Implementation is
   * optional, for informational purposes only.
   * @throws Exception
   */
  @Override public boolean addLink(String dbid, Link link, boolean noinverse) throws Exception {
    int edgeSize = link.data.length + 4 * 8;
    writer.write(link.id1 + "," + CommonStats.getShardId(edgeSize) + "\n");
    return true;
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
    return addLink(dbid, a, noinverse);
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
