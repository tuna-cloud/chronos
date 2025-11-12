package org.apache.chronos.cluster.meta;

import io.netty.util.Recycler;

public class Offset {

  public static final int MAX_META_DATA_ID = 0x3FFFFFFF;
  public static final int TOTAL_SIZE = 25;
  public static final int STATUS_NULL = 0;
  public static final int STATUS_NORMAL = 1;
  public static final int STATUS_DELETED = 2;

  /**
   * 0: NULL 1: NORMAL 2: DELETED
   */
  private byte status;
  private int blockId;
  private long offset;
  private int length;
  private long updated;

  private final Recycler.Handle<Offset> handle;

  public Offset(Recycler.Handle<Offset> handle) {
    this.handle = handle;
  }

  public void recycle() {
    status = -1;
    blockId = -1;
    offset = -1;
    length = -1;
    updated = -1;
    handle.recycle(this);
  }

  private static final Recycler<Offset> RECYCLER = new Recycler<Offset>() {
    @Override
    protected Offset newObject(Handle<Offset> handle) {
      return new Offset(handle);
    }
  };

  public static Offset create() {
    return RECYCLER.get();
  }

  public byte getStatus() {
    return status;
  }

  public void setStatus(byte status) {
    this.status = status;
  }

  public int getBlockId() {
    return blockId;
  }

  public void setBlockId(int blockId) {
    this.blockId = blockId;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public long getUpdated() {
    return updated;
  }

  public void setUpdated(long updated) {
    this.updated = updated;
  }
}
