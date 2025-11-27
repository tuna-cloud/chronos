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
import java.util.EnumSet;
import org.apache.commons.codec.digest.XXHash32;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TagsIndexChannel {

  private final static Logger log = LogManager.getLogger(TagsIndexChannel.class);
  private final String indexFilePath;
  private final int indexMaxCapacity;
  private final IStorageEngine engine;

  // --- 常量定义，取代魔术数字 ---
  private static final String TAG_INDEX_FILE = "TAGS.IDX";
  private static final int ENTRY_SIZE_BYTES = 32; // 每个索引条目固定大小
  private static final int STATUS_OFFSET = 0;
  private static final int BLOCK_ID_OFFSET = 4;
  private static final int BLOCK_OFFSET_OFFSET = 8;
  private static final int TAG_STRING_OFFSET = 12;
  private static final int MAX_TAG_LENGTH = 19; // 最大标签长度
  private static final int TAG_EXISTS_STATUS = 1;

  private FileChannel indexFileChannel;
  private MappedByteBuffer indexMappedByteBuffer;
  private ByteBuf indexByteBuf;

  // 使用 ThreadLocal 缓存哈希实例，提高性能并保证线程隔离
  private static final ThreadLocal<XXHash32> XXHASH_LOCAL = ThreadLocal.withInitial(() -> new XXHash32(0));


  public TagsIndexChannel(String filePath, int capacity, IStorageEngine engine) throws IOException {
    this.engine = engine;
    this.indexFilePath = filePath;
    this.indexMaxCapacity = capacity;
    this.init();
  }

  /**
   * 初始化索引文件和内存映射。
   */
  private void init() throws IOException {
    File indexFile = new File(indexFilePath + File.separator + TAG_INDEX_FILE);

    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

    long fileSize;

    if (!indexFile.exists()) {
      indexFileChannel = FileChannel.open(indexFile.toPath(), options);
      fileSize = getExpectedIndexFileSize();
      indexFileChannel.truncate(fileSize); // 预分配空间
      indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
      persist(); // 强制写入磁盘，确保空间分配生效
      log.info("Created new index file: {} with size {}", indexFile.getAbsolutePath(), fileSize);
    } else {
      indexFileChannel = FileChannel.open(indexFile.toPath(), options);
      fileSize = indexFileChannel.size(); // 获取现有文件大小
      indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
      log.info("Opened existing index file: {}", indexFile.getAbsolutePath());
    }
    indexByteBuf = Unpooled.wrappedBuffer(indexMappedByteBuffer);
  }

  /**
   * 强制将内存中的变更同步到磁盘。
   */
  private void persist() throws IOException {
    indexMappedByteBuffer.force();
    indexFileChannel.force(true);
  }

  /**
   * 计算预期的索引文件大小。
   */
  private long getExpectedIndexFileSize() {
    int size = engine.getSize();
    if (size < 1) {
      size = 1;
    }
    // 扩容计算方式：N百万 X 4 (倍数) X 10 (桶大小) X 32 (entry size)
    int num = (int) Math.ceil((double) size / indexMaxCapacity);
    // 使用常量和更易读的数字格式
    return num * 1_000_000L * 4 * 10 * ENTRY_SIZE_BYTES;
  }

  /**
   * 添加索引条目。该方法是线程安全的。
   */
  public synchronized void addIndex(String tag, int blockId, int blockOffset) throws IOException {
    if (tag == null) {
      return;
    }
    if (tag.length() > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException("Tag length cannot be greater than " + MAX_TAG_LENGTH);
    }

    // 检查并处理扩容（TODO实现细节）
    checkIndexFileCapacity();

    int pos = getPosition(tag);
    int status = indexByteBuf.getUnsignedByte(pos + STATUS_OFFSET);

    // reserve 3 bytes
    while (status == TAG_EXISTS_STATUS) { // tag exists, judge if hash is repeat
      String fileTag = CodecUtil.getString(indexByteBuf, pos + TAG_STRING_OFFSET);
      if (tag.equals(fileTag)) {
        // Found existing entry for this tag, update its block info
        break;
      }
      // Conflict detected, move to next slot (linear probing)
      pos = pos + ENTRY_SIZE_BYTES;
      status = indexByteBuf.getUnsignedByte(pos + STATUS_OFFSET);
    }

    // Write/Update the entry
    indexByteBuf.setByte(pos + STATUS_OFFSET, TAG_EXISTS_STATUS);
    indexByteBuf.setInt(pos + BLOCK_ID_OFFSET, blockId);
    indexByteBuf.setInt(pos + BLOCK_OFFSET_OFFSET, blockOffset);
    CodecUtil.setString(indexByteBuf, pos + TAG_STRING_OFFSET, tag);
  }

  /**
   * 移除索引条目。该方法是线程安全的。
   */
  public synchronized void removeIndex(String tag) throws IOException {
    if (tag == null) {
      return;
    }
    if (tag.length() > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException("Tag length cannot be greater than " + MAX_TAG_LENGTH);
    }

    checkIndexFileCapacity(); // 依然需要检查容量，尽管是删除操作

    int pos = getPosition(tag);
    final int startPos = pos;
    int status = indexByteBuf.getUnsignedByte(pos + STATUS_OFFSET);
    String fileTag = CodecUtil.getString(indexByteBuf, pos + TAG_STRING_OFFSET);

    // Find the exact tag using linear probing
    while (status == TAG_EXISTS_STATUS && !tag.equals(fileTag)) {
      pos = pos + ENTRY_SIZE_BYTES;
      status = indexByteBuf.getUnsignedByte(pos + STATUS_OFFSET);
      fileTag = CodecUtil.getString(indexByteBuf, pos + TAG_STRING_OFFSET);
    }

    // If found, clear it and re-shuffle subsequent entries in the collision chain
    if (status == TAG_EXISTS_STATUS && tag.equals(fileTag)) {
      clearIndexEntry(pos);

      // Try to move subsequent entries in the collision chain "up"
      int nextPos = pos + ENTRY_SIZE_BYTES;
      int nextStatus = indexByteBuf.getUnsignedByte(nextPos + STATUS_OFFSET);
      String nextFileTag = CodecUtil.getString(indexByteBuf, nextPos + TAG_STRING_OFFSET);
      int expectedPosOfNext = getPosition(nextFileTag);

      while (nextStatus == TAG_EXISTS_STATUS && expectedPosOfNext == startPos) {
        // Move entry up by copying data
        for (int i = 0; i < ENTRY_SIZE_BYTES; i++) {
          indexByteBuf.setByte(pos + i, indexByteBuf.getByte(nextPos + i));
        }
        clearIndexEntry(nextPos);

        // Advance pointers
        pos = nextPos;
        nextPos += ENTRY_SIZE_BYTES;

        // Check the next entry in the sequence
        nextStatus = indexByteBuf.getUnsignedByte(nextPos + STATUS_OFFSET);
        nextFileTag = CodecUtil.getString(indexByteBuf, nextPos + TAG_STRING_OFFSET);
        expectedPosOfNext = getPosition(nextFileTag);
      }
    }
  }

  private void clearIndexEntry(int pos) {
    for (int i = 0; i < ENTRY_SIZE_BYTES; i++) {
      indexByteBuf.setByte(pos + i, (byte) 0);
    }
  }

  /**
   * 获取标签对应的块ID和偏移量。该方法是线程安全的。
   */
  public synchronized Pair<Integer, Integer> getBlockOffset(String tag) throws IOException {
    if (tag == null || tag.length() > MAX_TAG_LENGTH) {
      return null;
    }

    checkIndexFileCapacity();
    int pos = getPosition(tag);
    int status = indexByteBuf.getUnsignedByte(pos + STATUS_OFFSET);

    while (status == TAG_EXISTS_STATUS) {
      String fileTag = CodecUtil.getString(indexByteBuf, pos + TAG_STRING_OFFSET);
      if (tag.equals(fileTag)) {
        // Found the exact match
        int block = indexByteBuf.getInt(pos + BLOCK_ID_OFFSET);
        int offset = indexByteBuf.getInt(pos + BLOCK_OFFSET_OFFSET);
        return Pair.of(block, offset);
      }
      // Linear probe to next entry
      pos = pos + ENTRY_SIZE_BYTES;
      status = indexByteBuf.getUnsignedByte(pos + STATUS_OFFSET);
    }

    return null; // Not found
  }

  /**
   * 检查索引文件容量。**重要：实际的扩容和rehash逻辑需要在此处实现**
   */
  private void checkIndexFileCapacity() throws IOException {
    long expectedSize = getExpectedIndexFileSize();
    if (indexFileChannel.size() < expectedSize) {
      log.warn("Index file capacity insufficient. Current size: {}, Expected size: {}. Triggering expansion...",
          indexFileChannel.size(), expectedSize);

      // TODO: Implement the index expansion logic here.
      // 1. Create a new, larger MappedByteBuffer/File
      // 2. Iterate through ALL entries in the old buffer
      // 3. Re-calculate hash positions for the new capacity
      // 4. Write all entries into the new buffer
      // 5. Swap the MappedByteBuffers/FileChannels
      // This is a complex operation that needs careful implementation to be atomic and safe.

//      throw new IOException("Index capacity reached. Expansion logic required.");
    }
  }

  /**
   * 根据标签计算其在文件中的起始哈希位置。
   */
  public int getPosition(String tag) {
    if (tag == null) {
      return -1;
    }
    // Use ThreadLocal cached hash instance
    XXHash32 xxHash = XXHASH_LOCAL.get();
    xxHash.reset();
    byte[] buf = tag.getBytes(StandardCharsets.UTF_8);
    xxHash.update(buf, 0, buf.length);

    // Calculate position: (Hash % BucketCount) * BucketSize * EntrySize
    // Note: The original logic seems to use indexMaxCapacity as part of the calculation,
    // implying indexMaxCapacity relates to the number of *initial* primary hash buckets.
    long hashValue = xxHash.getValue() & 0xFFFFFFFFL; // Ensure positive hash value

    // The calculation assumes a specific structure (e.g., 10 slots per initial bucket)
    return (int) (hashValue % indexMaxCapacity) * 10 * ENTRY_SIZE_BYTES;
  }

  // 可以添加一个 close() 方法来释放资源
  public void close() throws IOException {
    if (indexFileChannel != null) {
      indexFileChannel.close();
    }
    // MappedByteBuffer的释放比较复杂，依赖GC或使用Cleaner API（Java 9+）
    // 对于Java 8，通常依赖GC，或者使用Unsafe hack，这里省略复杂实现。
    indexByteBuf = null;
    indexMappedByteBuffer = null;
  }
}
