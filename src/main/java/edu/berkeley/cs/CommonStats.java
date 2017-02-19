package edu.berkeley.cs;

import java.util.concurrent.atomic.AtomicLong;

public class CommonStats {
  public static long MAX_SHARD_SIZE = 1024 * 1024 * 1024;

  public static AtomicLong currentShardSize = new AtomicLong(0L);
  public static AtomicLong shardId = new AtomicLong(0L);

  public static long getShardId(long dataSize) {
    long shardId = CommonStats.shardId.get();
    currentShardSize.addAndGet(dataSize);
    long shardSize;
    boolean success = false;
    while ((shardSize = currentShardSize.get()) > MAX_SHARD_SIZE && !success) {
      success = currentShardSize.compareAndSet(shardSize, 0L);
      if (success)
        shardId = CommonStats.shardId.addAndGet(1L);
    }
    return shardId;
  }
}
