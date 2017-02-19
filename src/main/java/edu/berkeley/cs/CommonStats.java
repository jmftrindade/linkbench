package edu.berkeley.cs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

public class CommonStats {
  public static long MAX_SHARD_SIZE = 2L * 1024L * 1024L * 1024L;

  public static long currentShardSize = 0L;
  public static long shardId = 0L;

  public static BufferedWriter writer;

  static {
    try {
      writer = new BufferedWriter(new FileWriter("linkbench.stats"));
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(0);
    }
  }

  public void log(Logger log) {
    log.info("ShardID = " + shardId + " Shard Size = " + currentShardSize);
  }

  public static long getShardId(long dataSize) {
    currentShardSize += dataSize;
    if (currentShardSize > MAX_SHARD_SIZE) {
      currentShardSize = 0;
      shardId++;
    }
    return shardId;
  }
}
