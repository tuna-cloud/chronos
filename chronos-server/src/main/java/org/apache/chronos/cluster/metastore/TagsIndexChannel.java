package org.apache.chronos.cluster.metastore;

import com.apache.chronos.protocol.codec.CodecUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.chronos.common.FileUtil;
import org.apache.commons.codec.digest.XXHash32;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TagsIndexChannel {

  private final static Logger log = LogManager.getLogger(TagsIndexChannel.class);
  private final String indexFilePath;
  // 这个成员变量将存储当前的“哈希模数基数”（替代旧的 indexMaxCapacity）
  // 它需要在扩容时被更新
  private int currentHashCapacity;
  private final IStorageEngine engine;

  // --- 常量定义，取代魔术数字 ---
  private static final String TAG_INDEX_FILE = "TAGS.IDX";
  private static final String TAG_INDEX_FILE_TMP = "TAGS.IDX.TMP";
  private static final int CAPACITY_UNIT = 10000; // 以万为单位
  // 定义目标负载因子阈值 (例如 50%)
  private static final double LOAD_FACTOR_THRESHOLD = 0.25;
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
  private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();


  public TagsIndexChannel(String filePath, IStorageEngine engine) throws IOException {
    this.engine = engine;
    this.indexFilePath = filePath;
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
      fileSize = getExpectedIndexFileSize(engine.getSize());
      indexFileChannel.truncate(fileSize); // 预分配空间
      this.currentHashCapacity = calculateCapacityFromSize(fileSize);
      indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
      persist(); // 强制写入磁盘，确保空间分配生效
      log.info("Created new index file: {} with size {}", indexFile.getAbsolutePath(), fileSize);
    } else {
      indexFileChannel = FileChannel.open(indexFile.toPath(), options);
      fileSize = indexFileChannel.size(); // 获取现有文件大小
      this.currentHashCapacity = calculateCapacityFromSize(fileSize);
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
  protected long getExpectedIndexFileSize(int dataSize) {
    int currentDataSize = dataSize; // 当前实际数据量
    if (currentDataSize < 1) {
      currentDataSize = 1;
    }

    // 计算理论上所需的最小容量（例如 2000条数据 -> 4000）
    int minNeededCapacity = (int) Math.ceil(currentDataSize / LOAD_FACTOR_THRESHOLD);

    // 将最小所需容量向上取整到最接近的 CAPACITY_UNIT (10000) 的倍数
    // 例如 4000 -> 10000; 12000 -> 20000; 105000 -> 110000
    int alignedCapacity = (int) Math.ceil((double) minNeededCapacity / CAPACITY_UNIT) * CAPACITY_UNIT;

    // 确保计算出的容量至少大于当前数据量，以维持负载因子
    if (alignedCapacity < minNeededCapacity) {
      alignedCapacity += CAPACITY_UNIT;
    }

    // 如果对齐后的容量小于当前文件已有的容量，则保持不变，防止缩容
    if (alignedCapacity < this.currentHashCapacity) {
      alignedCapacity = this.currentHashCapacity;
    }
    // --- 增加上限检查 ---
    long expectedSize = (long) alignedCapacity * 10 * ENTRY_SIZE_BYTES;
    if (expectedSize > Integer.MAX_VALUE) {
      log.error("Index file size calculation exceeded the MappedByteBuffer limit. Calculated size: {} bytes, Max limit: {} bytes.", expectedSize, Integer.MAX_VALUE);
      throw new MemMapSpaceExceedException("Index file size exceeds the maximum allowed MappedByteBuffer size (approx 2GB). Cannot allocate file.");
    }
    // 计算文件大小： 容量 * 10 (槽位/桶) * 32 (字节/槽位)
    return (long) alignedCapacity * 10 * ENTRY_SIZE_BYTES;
  }

  /**
   * 辅助方法：从文件大小反推主桶容量（需要与 getExpectedIndexFileSize 逻辑对称）
   */
  protected int calculateCapacityFromSize(long fileSize) {
    // 根据文件大小反推出当时分配的容量 (alignedCapacity)
    // fileSize = capacity * 10 * ENTRY_SIZE_BYTES

    long totalSlots = fileSize / ENTRY_SIZE_BYTES;
    int capacity = (int) (totalSlots / 10);

    // 理论上，这个容量应该已经是 CAPACITY_UNIT (10000) 的倍数了
    // 我们可以添加一个断言或检查来确保逻辑正确
    if (capacity % CAPACITY_UNIT != 0) {
      log.warn("Calculated capacity {} is not a multiple of the expected unit {}. Data inconsistency possible.",
          capacity, CAPACITY_UNIT);
    }
    return capacity;
  }

  /**
   * 添加索引条目。该方法是线程安全的。
   */
  public void addIndex(String tag, int blockId, int blockOffset) throws IOException {
    if (tag == null) {
      return;
    }
    if (tag.length() > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException("Tag length cannot be greater than " + MAX_TAG_LENGTH);
    }

    try {
      writeLock.lock();
      // 检查并处理扩容
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
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 移除索引条目。该方法是线程安全的。
   */
  public void removeIndex(String tag) throws IOException {
    if (tag == null) {
      return;
    }
    if (tag.length() > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException("Tag length cannot be greater than " + MAX_TAG_LENGTH);
    }

    try {
      writeLock.lock();
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
    } finally {
      writeLock.unlock();
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
  public Pair<Integer, Integer> getBlockOffset(String tag) throws IOException {
    if (tag == null || tag.length() > MAX_TAG_LENGTH) {
      return null;
    }

    try {
      readLock.lock();
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
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 检查索引文件容量。**重要：实际的扩容和rehash逻辑需要在此处实现**
   */
  private void checkIndexFileCapacity() throws IOException {
    long expectedSize = getExpectedIndexFileSize(engine.getSize());
    if (indexFileChannel.size() < expectedSize) {
      // 执行 Rehash 扩容操作
      rehashAndExpand(expectedSize);
      log.info("Index expansion complete. New size: {}", expectedSize);
    }
  }

  /**
   * 执行实际的 Rehash 扩容逻辑。 这是一个关键且复杂的方法，需要仔细实现以确保数据一致性和线程安全。
   */
  private void rehashAndExpand(long newSize) throws IOException {
    Path indexPath = Path.of(indexFilePath, TAG_INDEX_FILE);
    Path tempIndexPath = Path.of(indexFilePath, TAG_INDEX_FILE_TMP);

    int newCapacity = calculateCapacityFromSize(newSize);
    log.warn("Index file capacity insufficient. Current size: {} capacity: {}, Expected size: {} capacity: {}. Triggering expansion...",
        indexFileChannel.size(), this.currentHashCapacity, newSize, newCapacity);

    // 1. 创建一个新的临时文件，并预分配空间
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    try (FileChannel tempChannel = FileChannel.open(tempIndexPath, options)) {
      tempChannel.truncate(newSize);
      // 确保空间分配生效
      tempChannel.force(true);

      // 映射新的 MappedByteBuffer
      MappedByteBuffer newMappedBuffer = tempChannel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
      ByteBuf newByteBuf = Unpooled.wrappedBuffer(newMappedBuffer);

      // 2. 迭代旧索引中的所有条目并迁移
      // 注意：这里使用了旧的 indexByteBuf，必须确保在持有写锁时执行此操作
      long currentPos = 0;
      long oldSize = indexFileChannel.size();

      while (currentPos < oldSize) {
        int status = indexByteBuf.getUnsignedByte((int) currentPos + STATUS_OFFSET);
        if (status == TAG_EXISTS_STATUS) {
          // 读取旧条目数据
          int blockId = indexByteBuf.getInt((int) currentPos + BLOCK_ID_OFFSET);
          int blockOffset = indexByteBuf.getInt((int) currentPos + BLOCK_OFFSET_OFFSET);
          String tag = CodecUtil.getString(indexByteBuf, (int) currentPos + TAG_STRING_OFFSET);

          // 将数据写入新索引（需要一个方法来安全写入新结构）
          writeNewIndexEntry(newByteBuf, tag, blockId, blockOffset, newCapacity, newSize);
        }
        currentPos += ENTRY_SIZE_BYTES;
      }

      // 3. 强制新数据写入磁盘
      newMappedBuffer.force();
      tempChannel.force(true);

      // 4. 关闭旧的资源并释放 MappedByteBuffer
      indexFileChannel.close();
      FileUtil.clean(indexMappedByteBuffer); // 使用辅助方法释放内存映射

      // 5. 原子替换文件
      Files.move(tempIndexPath, indexPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

      // 6. 重新初始化类的成员变量以使用新文件
      // 注意：init() 方法需要调整为不重复创建文件，而是打开已有的新文件
      initAfterExpansion();
    } catch (Exception e) {
      // 如果发生异常，需要清理临时文件，并可能需要恢复旧状态（如果可能）
      log.error("Index expansion failed, attempting cleanup.", e);
      Files.deleteIfExists(tempIndexPath);
      throw new IOException("Failed to expand index file", e);
    }
  }

  /**
   * 辅助方法：重新初始化字段以使用新的文件（在原子替换文件后调用）
   */
  private void initAfterExpansion() throws IOException {
    File indexFile = new File(indexFilePath + File.separator + TAG_INDEX_FILE);
    EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE);

    indexFileChannel = FileChannel.open(indexFile.toPath(), options);
    long fileSize = indexFileChannel.size();
    indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    indexByteBuf = Unpooled.wrappedBuffer(indexMappedByteBuffer);
    this.currentHashCapacity = calculateCapacityFromSize(fileSize);
    // Note: The indexMaxCapacity variable in the class might need updating if it changes during expansion
  }


  /**
   * 辅助方法：将条目写入新的 ByteBuf（使用新的文件容量进行哈希计算） 接受新的容量作为参数
   */
  private void writeNewIndexEntry(ByteBuf targetBuf, String tag, int blockId, int blockOffset, int newCapacity, long newFileSize) {

    // !!! 关键修复点 !!!
    // 使用新的 newCapacity 参数来计算哈希位置
    int newPos = getPosition(tag, newCapacity, ENTRY_SIZE_BYTES);

    // 标准的线性探测写入逻辑
    int status = targetBuf.getUnsignedByte(newPos + STATUS_OFFSET);
    while (status == TAG_EXISTS_STATUS) {
      newPos += ENTRY_SIZE_BYTES;
      // 需要检查是否超出新的文件边界，防止越界写入
      if (newPos >= newFileSize) {
        // 这是严重错误，说明容量计算有问题或者哈希冲突过多导致溢出
        throw new IndexOutOfBoundsException("Rehash overflowed new file capacity at position: " + newPos);
      }
      status = targetBuf.getUnsignedByte(newPos + STATUS_OFFSET);
    }

    targetBuf.setByte(newPos + STATUS_OFFSET, TAG_EXISTS_STATUS);
    targetBuf.setInt(newPos + BLOCK_ID_OFFSET, blockId);
    targetBuf.setInt(newPos + BLOCK_OFFSET_OFFSET, blockOffset);
    CodecUtil.setString(targetBuf, newPos + TAG_STRING_OFFSET, tag);
  }

  /**
   * 根据标签和指定的容量参数计算在文件中的起始哈希位置。
   */
  public int getPosition(String tag, int capacity, int entrySizeBytes) {
    if (tag == null) {
      return -1;
    }
    // 使用 ThreadLocal 缓存哈希实例
    XXHash32 xxHash = XXHASH_LOCAL.get();
    xxHash.reset();
    byte[] buf = tag.getBytes(StandardCharsets.UTF_8);
    xxHash.update(buf, 0, buf.length);

    long hashValue = xxHash.getValue() & 0xFFFFFFFFL; // 确保正数

    // 原始计算逻辑： (Hash % capacity) * 10 * ENTRY_SIZE_BYTES
    // 假设这里的 capacity 对应于 indexMaxCapacity，即主桶数量
    return (int) (hashValue % capacity) * 10 * entrySizeBytes;
  }

  /**
   * 原有的 getPosition 方法，现在调用新的方法，用于非扩容场景。
   */
  public int getPosition(String tag) {
    // 默认使用当前类的成员变量作为容量参数
    return getPosition(tag, this.currentHashCapacity, ENTRY_SIZE_BYTES);
  }

  // 可以添加一个 close() 方法来释放资源
  public void close() throws IOException {
    if (indexFileChannel != null) {
      indexFileChannel.close();
    }
    // MappedByteBuffer的释放比较复杂，依赖GC或使用Cleaner API（Java 9+）
    // 对于Java 8，通常依赖GC，或者使用Unsafe hack，这里省略复杂实现。
    FileUtil.clean(indexMappedByteBuffer);
    indexByteBuf = null;
    indexMappedByteBuffer = null;
  }
}
