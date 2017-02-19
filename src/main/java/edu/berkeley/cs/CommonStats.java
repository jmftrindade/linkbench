package edu.berkeley.cs;

public class CommonStats {
  public static long MAX_SHARD_SIZE = 2L * 1024L * 1024L * 1024L;

  public static long currentShardSize = 0L;
  public static long shardId = 0L;

  public static long getShardId(long dataSize) {
    currentShardSize += dataSize;
    if (currentShardSize > MAX_SHARD_SIZE) {
      currentShardSize = 0;
      shardId++;
    }
    return shardId;
  }
}
