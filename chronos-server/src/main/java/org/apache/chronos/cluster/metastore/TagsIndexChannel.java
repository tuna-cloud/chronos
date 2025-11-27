package org.apache.chronos.cluster.metastore;

import com.apache.chronos.protocol.codec.CodecUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import org.apache.commons.codec.digest.XXHash32;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TagsIndexChannel {
  private final static Logger log = LogManager.getLogger(TagsIndexChannel.class);
  private final String indexFilePath;
  private final int indexMaxCapacity;
  private static final String TAG_INDEX_FILE = "TAGS.IDX";
  private FileChannel indexFileChannel;
  private MappedByteBuffer indexMappedByteBuffer;
  private ByteBuf indexByteBuf;
  private final IStorageEngine engine;

  public TagsIndexChannel(String filePath, int capacity, IStorageEngine engine) throws Exception {
    this.engine = engine;
    this.indexFilePath = filePath;
    this.indexMaxCapacity = capacity;
    this.init();
  }

  /**
   * 当设备数量小于100万时，索引空间容量为400万桶，每个桶的大小为10， 总大小 100万 X 4 X 10 X 32 bytes = 1.19G 每当设备新增100万，将触发一次扩容，扩容计算方式： N百万 X 4 。。。。
   */
  private void init() throws Exception {
    File indexFile = new File(indexFilePath + File.separator + TAG_INDEX_FILE);
    if (!indexFile.exists()) {
      indexFileChannel = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      indexFileChannel.truncate(getExpectedIndexFileSize());
      indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, getExpectedIndexFileSize());
      persist();
    } else {
      indexFileChannel = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexFileChannel.size());
    }
    indexByteBuf = Unpooled.wrappedBuffer(indexMappedByteBuffer);
  }

  private void persist() throws IOException {
    indexMappedByteBuffer.force();
    indexFileChannel.force(true);
  }

  private long getExpectedIndexFileSize() {
    int size = engine.getSize();
    if (size < 1) {
      size = 1;
    }
    int num = Math.ceilDiv(size, indexMaxCapacity);
    return num * 100_0000L * 4 * 10 * 32;
  }

  public void addIndex(String tag, int blockId, int blockOffset) throws Exception {
    if (tag == null) {
      return;
    }
    if (tag.length() > 19) {
      throw new RuntimeException("tag's length can not be greater than 15");
    }
    checkIndexFileCapacity();

    int pos = getPosition(tag);
    int status = indexByteBuf.getUnsignedByte(pos);
    // reserve 3 bytes
    if (status == 1) { // tag exits, judge if hash is repeat
      String fileTag = CodecUtil.getString(indexByteBuf, pos + 12);
      while (status == 1 && !tag.equals(fileTag)) {
        pos = pos + 32;
        status = indexByteBuf.getUnsignedByte(pos);
        fileTag = CodecUtil.getString(indexByteBuf, pos + 12);
      }
      indexByteBuf.setByte(pos, 1);
      indexByteBuf.setInt(pos + 4, blockId);
      indexByteBuf.setInt(pos + 8, blockOffset);
      CodecUtil.setString(indexByteBuf, pos + 12, tag);
    } else { // tag not exits
      indexByteBuf.setByte(pos, 1);
      indexByteBuf.setInt(pos + 4, blockId);
      indexByteBuf.setInt(pos + 8, blockOffset);
      CodecUtil.setString(indexByteBuf, pos + 12, tag);
    }
  }

  public void removeIndex(String tag) throws Exception {
    if (tag == null) {
      return;
    }
    if (tag.length() > 19) {
      throw new RuntimeException("tag's length can not be greater than 15");
    }
    checkIndexFileCapacity();
    int pos = getPosition(tag);
    int startPos = pos;
    int status = indexByteBuf.getUnsignedByte(pos);
    String fileTag = CodecUtil.getString(indexByteBuf, pos + 12);
    while (status == 1 && !tag.equals(fileTag)) {
      pos = pos + 32;
      status = indexByteBuf.getUnsignedByte(pos);
      fileTag = CodecUtil.getString(indexByteBuf, pos + 12);
    }
    if (status == 1 && tag.equals(fileTag)) {
      // found
      clearIndexEntry(pos);
      // try move next entry to here.
      pos += 32;
      status = indexByteBuf.getUnsignedByte(pos);
      fileTag = CodecUtil.getString(indexByteBuf, pos + 12);
      int expectedPos = getPosition(fileTag);
      while (status == 1 && expectedPos == startPos) {
        for(int i = 0; i < 32; i++) {
          indexByteBuf.setByte(pos - 32 + i, indexByteBuf.getByte(pos + i));
        }
        clearIndexEntry(pos);
        pos += 32;
        // 同一个桶内的数据，顺序迁移
        status = indexByteBuf.getUnsignedByte(pos);
        fileTag = CodecUtil.getString(indexByteBuf, pos + 12);
        expectedPos = getPosition(fileTag);
      }
    }
  }

  private void clearIndexEntry(int pos) {
    for (int i = 0; i < 32; i++) {
      indexByteBuf.setByte(pos + i, 0);
    }
  }

  public Pair<Integer, Integer> getBlockOffset(String tag) throws IOException {
    if (tag == null) {
      return null;
    }
    if (tag.length() > 19) {
      throw new RuntimeException("tag's length can not be greater than 15");
    }
    checkIndexFileCapacity();
    int pos = getPosition(tag);
    int status = indexByteBuf.getUnsignedByte(pos);
    if (status == 1) {
      String fileTag =CodecUtil.getString(indexByteBuf, pos + 12);
      while (status != 0 && !tag.equals(fileTag)) {
        pos = pos + 32;
        status = indexByteBuf.getUnsignedByte(pos);
        fileTag = status == 1 ? CodecUtil.getString(indexByteBuf, pos + 12) : null;
      }
      if (status == 1 && tag.equals(fileTag)) {
        int block = indexByteBuf.getInt(pos + 4);
        int offset = indexByteBuf.getInt(pos + 8);
        return Pair.of(block, offset);
      }
    }
    return null;
  }

  private void checkIndexFileCapacity() throws IOException {
    long size = getExpectedIndexFileSize();
    if (indexFileChannel.size() < size) {
      // TODO expand the index file size and reindex all tags.
      System.out.println("checkIndexFileCapacity");
    }
  }

  public int getPosition(String tag) {
    if (tag == null) {
      return -1;
    }
    XXHash32 xxHash = new XXHash32(0);
    byte[] buf = tag.getBytes(StandardCharsets.UTF_8);
    xxHash.update(buf, 0, buf.length);
    return (int) (xxHash.getValue() % indexMaxCapacity) * 10 * 32;
  }
}
