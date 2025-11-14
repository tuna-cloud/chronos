package org.apache.chronos.cluster.metastore;

public class TagsIndexEntry {
  private String tag;
  private int indexOffset;
  private int blockId;
  private int offset;
  private int size;

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public int getIndexOffset() {
    return indexOffset;
  }

  public void setIndexOffset(int indexOffset) {
    this.indexOffset = indexOffset;
  }

  public int getBlockId() {
    return blockId;
  }

  public void setBlockId(int blockId) {
    this.blockId = blockId;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }
}
