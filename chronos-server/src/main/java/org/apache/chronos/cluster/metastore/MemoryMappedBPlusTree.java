package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于Memory Mapped File的高性能B+树实现
 */
public class MemoryMappedBPlusTree {

  private static final int DEFAULT_ORDER = 4;
  private static final int PAGE_SIZE = 4096; // 4KB页大小
  private static final int NODE_HEADER_SIZE = 16;
  private static final int KEY_SIZE = 8; // long类型key
  private static final int POINTER_SIZE = 8; // long类型指针

  // 节点类型
  private static final byte LEAF_NODE = 0;
  private static final byte INTERNAL_NODE = 1;

  // 文件头结构
  private static final int MAGIC_NUMBER_OFFSET = 0;
  private static final int ROOT_POINTER_OFFSET = 4;
  private static final int FIRST_LEAF_OFFSET = 12;
  private static final int NEXT_PAGE_OFFSET = 20;
  private static final int FILE_HEADER_SIZE = 32;

  private final int order;
  private final int maxKeys;
  private final Path filePath;
  private RandomAccessFile raf;
  private FileChannel channel;
  private MappedByteBuffer mappedBuffer;
  private final AtomicLong nextPageId;

  public MemoryMappedBPlusTree(String filePath) throws IOException {
    this(filePath, DEFAULT_ORDER);
  }

  public MemoryMappedBPlusTree(String filePath, int order) throws IOException {
    this.order = order;
    this.maxKeys = order - 1;
    this.filePath = Paths.get(filePath);

    boolean fileExists = Files.exists(this.filePath);

    // 打开或创建文件
    this.raf = new RandomAccessFile(this.filePath.toFile(), "rw");
    this.channel = raf.getChannel();

    // 映射整个文件到内存
    long fileSize = Math.max(channel.size(), PAGE_SIZE * 16); // 至少16页
    this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

    if (!fileExists) {
      initializeFile();
    }

    // 读取next page id
    this.nextPageId = new AtomicLong(mappedBuffer.getLong(NEXT_PAGE_OFFSET));
  }

  private void initializeFile() {
    // 写入magic number
    mappedBuffer.putInt(MAGIC_NUMBER_OFFSET, 0xBP1US); // B+1 US

    // 创建根节点
    long rootPointer = allocatePage();
    ByteBuf rootNode = createLeafNode();
    writeNode(rootPointer, rootNode);
    rootNode.release();

    mappedBuffer.putLong(ROOT_POINTER_OFFSET, rootPointer);
    mappedBuffer.putLong(FIRST_LEAF_OFFSET, rootPointer);
    mappedBuffer.putLong(NEXT_PAGE_OFFSET, rootPointer + PAGE_SIZE);

    mappedBuffer.force();
  }

  private long allocatePage() {
    long pageId = nextPageId.getAndAdd(PAGE_SIZE);

    // 如果文件需要扩展
    if (pageId + PAGE_SIZE > mappedBuffer.capacity()) {
      try {
        extendFile();
      } catch (IOException e) {
        throw new RuntimeException("Failed to extend file", e);
      }
    }

    return pageId;
  }

  private void extendFile() throws IOException {
    long newSize = mappedBuffer.capacity() * 2;

    // 取消当前映射
    unmap(mappedBuffer);

    // 扩展文件
    channel.truncate(newSize);

    // 重新映射
    mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
  }

  private ByteBuf createLeafNode() {
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(PAGE_SIZE);
    initializeNode(buf, LEAF_NODE, 0, -1, -1);
    return buf;
  }

  private ByteBuf createInternalNode() {
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(PAGE_SIZE);
    initializeNode(buf, INTERNAL_NODE, 0, -1, -1);
    return buf;
  }

  private void initializeNode(ByteBuf node, byte nodeType, int keyCount, long nextPointer, long prevPointer) {
    node.writeByte(nodeType);
    node.writeInt(keyCount);
    node.writeLong(nextPointer);
    node.writeLong(prevPointer);

    // 初始化键和指针区域
    int remaining = PAGE_SIZE - node.writerIndex();
    for (int i = 0; i < remaining; i++) {
      node.writeByte(0);
    }
  }

  public void insert(long key, String value) throws IOException {
    long rootPointer = mappedBuffer.getLong(ROOT_POINTER_OFFSET);
    InsertResult result = insertRecursive(rootPointer, key, value);

    if (result.newChildPointer != -1) {
      // 根节点分裂，创建新的根节点
      long newRootPointer = allocatePage();
      ByteBuf newRoot = createInternalNode();

      // 设置第一个键和两个子指针
      newRoot.setInt(4, 1); // keyCount = 1
      setKeyInInternal(newRoot, 0, result.newChildKey);
      setPointerInInternal(newRoot, 0, rootPointer);
      setPointerInInternal(newRoot, 1, result.newChildPointer);

      writeNode(newRootPointer, newRoot);
      newRoot.release();

      // 更新根指针
      mappedBuffer.putLong(ROOT_POINTER_OFFSET, newRootPointer);
      mappedBuffer.force();
    }
  }

  private InsertResult insertRecursive(long nodePointer, long key, String value) throws IOException {
    ByteBuf node = readNode(nodePointer);
    try {
      byte nodeType = node.getByte(0);

      if (nodeType == LEAF_NODE) {
        return insertIntoLeaf(nodePointer, node, key, value);
      } else {
        return insertIntoInternal(nodePointer, node, key, value);
      }
    } finally {
      node.release();
    }
  }

  private InsertResult insertIntoLeaf(long nodePointer, ByteBuf node, long key, String value) throws IOException {
    int keyCount = node.getInt(4);
    int insertPos = findInsertPositionInLeaf(node, key, keyCount);

    // 检查是否重复键
    if (insertPos < keyCount && getKeyFromLeaf(node, insertPos) == key) {
      // 更新现有值
      setValueInLeaf(node, insertPos, value);
      writeNode(nodePointer, node);
      return new InsertResult(-1, -1);
    }

    if (keyCount < maxKeys) {
      // 节点未满，直接插入
      insertKeyValueIntoLeaf(node, key, value, insertPos, keyCount);
      node.setInt(4, keyCount + 1);
      writeNode(nodePointer, node);
      return new InsertResult(-1, -1);
    } else {
      // 节点分裂
      return splitLeafNode(nodePointer, node, key, value, insertPos);
    }
  }

  private InsertResult insertIntoInternal(long nodePointer, ByteBuf node, long key, String value) throws IOException {
    int keyCount = node.getInt(4);
    int childIndex = findChildIndex(node, key, keyCount);
    long childPointer = getPointerFromInternal(node, childIndex);

    InsertResult childResult = insertRecursive(childPointer, key, value);

    if (childResult.newChildPointer == -1) {
      return new InsertResult(-1, -1); // 没有分裂
    }

    // 子节点分裂，需要在当前节点插入新的键和指针
    if (keyCount < maxKeys) {
      insertKeyPointerIntoInternal(node, childResult.newChildKey, childResult.newChildPointer, childIndex, keyCount);
      node.setInt(4, keyCount + 1);
      writeNode(nodePointer, node);
      return new InsertResult(-1, -1);
    } else {
      return splitInternalNode(nodePointer, node, childResult.newChildKey, childResult.newChildPointer, childIndex);
    }
  }

  private InsertResult splitLeafNode(long nodePointer, ByteBuf oldLeaf, long newKey, String newValue, int insertPos) throws IOException {
    int splitIndex = order / 2;
    long newLeafPointer = allocatePage();
    ByteBuf newLeaf = createLeafNode();

    // 设置链表指针
    long nextPointer = oldLeaf.getLong(8);
    newLeaf.setLong(8, nextPointer);
    newLeaf.setLong(16, nodePointer);
    oldLeaf.setLong(8, newLeafPointer);

    // 重新分配键值对
    List<Long> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();

    int oldKeyCount = oldLeaf.getInt(4);

    // 收集所有键值对
    for (int i = 0; i < oldKeyCount; i++) {
      keys.add(getKeyFromLeaf(oldLeaf, i));
      values.add(getValueFromLeaf(oldLeaf, i));
    }

    // 插入新键值对
    keys.add(insertPos, newKey);
    values.add(insertPos, newValue);

    // 清空原节点并写入前半部分
    oldLeaf.setInt(4, splitIndex);
    for (int i = 0; i < splitIndex; i++) {
      setKeyInLeaf(oldLeaf, i, keys.get(i));
      setValueInLeaf(oldLeaf, i, values.get(i));
    }

    // 写入新节点的后半部分
    newLeaf.setInt(4, keys.size() - splitIndex);
    for (int i = 0; i < keys.size() - splitIndex; i++) {
      setKeyInLeaf(newLeaf, i, keys.get(splitIndex + i));
      setValueInLeaf(newLeaf, i, values.get(splitIndex + i));
    }

    writeNode(nodePointer, oldLeaf);
    writeNode(newLeafPointer, newLeaf);
    newLeaf.release();

    return new InsertResult(keys.get(splitIndex), newLeafPointer);
  }

  private InsertResult splitInternalNode(long nodePointer, ByteBuf oldNode, long newKey, long newPointer, int insertPos) throws IOException {
    int splitIndex = order / 2;
    long newNodePointer = allocatePage();
    ByteBuf newNode = createInternalNode();

    List<Long> keys = new ArrayList<>();
    List<Long> pointers = new ArrayList<>();

    int oldKeyCount = oldNode.getInt(4);

    // 收集所有键和指针
    for (int i = 0; i < oldKeyCount; i++) {
      keys.add(getKeyFromInternal(oldNode, i));
    }
    for (int i = 0; i <= oldKeyCount; i++) {
      pointers.add(getPointerFromInternal(oldNode, i));
    }

    // 插入新键和指针
    keys.add(insertPos, newKey);
    pointers.add(insertPos + 1, newPointer);

    long promoteKey = keys.get(splitIndex);

    // 写入原节点的前半部分
    oldNode.setInt(4, splitIndex);
    for (int i = 0; i < splitIndex; i++) {
      setKeyInInternal(oldNode, i, keys.get(i));
    }
    for (int i = 0; i <= splitIndex; i++) {
      setPointerInInternal(oldNode, i, pointers.get(i));
    }

    // 写入新节点的后半部分
    newNode.setInt(4, keys.size() - splitIndex - 1);
    for (int i = 0; i < keys.size() - splitIndex - 1; i++) {
      setKeyInInternal(newNode, i, keys.get(splitIndex + 1 + i));
    }
    for (int i = 0; i < keys.size() - splitIndex; i++) {
      setPointerInInternal(newNode, i, pointers.get(splitIndex + 1 + i));
    }

    writeNode(nodePointer, oldNode);
    writeNode(newNodePointer, newNode);
    newNode.release();

    return new InsertResult(promoteKey, newNodePointer);
  }

  public String search(long key) throws IOException {
    long leafPointer = findLeaf(key);
    ByteBuf leafNode = readNode(leafPointer);
    try {
      return searchInLeaf(leafNode, key);
    } finally {
      leafNode.release();
    }
  }

  private long findLeaf(long key) throws IOException {
    long currentNodePointer = mappedBuffer.getLong(ROOT_POINTER_OFFSET);

    while (true) {
      ByteBuf node = readNode(currentNodePointer);
      try {
        byte nodeType = node.getByte(0);

        if (nodeType == LEAF_NODE) {
          return currentNodePointer;
        }

        int keyCount = node.getInt(4);
        int childIndex = findChildIndex(node, key, keyCount);
        currentNodePointer = getPointerFromInternal(node, childIndex);
      } finally {
        node.release();
      }
    }
  }

  private int findChildIndex(ByteBuf node, long key, int keyCount) {
    for (int i = 0; i < keyCount; i++) {
      if (key < getKeyFromInternal(node, i)) {
        return i;
      }
    }
    return keyCount;
  }

  private String searchInLeaf(ByteBuf leafNode, long key) {
    int keyCount = leafNode.getInt(4);

    for (int i = 0; i < keyCount; i++) {
      long currentKey = getKeyFromLeaf(leafNode, i);
      if (currentKey == key) {
        return getValueFromLeaf(leafNode, i);
      }
      if (currentKey > key) {
        break;
      }
    }

    return null;
  }

  public List<String> rangeSearch(long startKey, long endKey) throws IOException {
    List<String> results = new ArrayList<>();
    long currentLeafPointer = findLeaf(startKey);

    while (currentLeafPointer != -1) {
      ByteBuf leafNode = readNode(currentLeafPointer);
      try {
        int keyCount = leafNode.getInt(4);
        boolean foundInRange = false;

        for (int i = 0; i < keyCount; i++) {
          long key = getKeyFromLeaf(leafNode, i);
          if (key >= startKey && key <= endKey) {
            results.add(getValueFromLeaf(leafNode, i));
            foundInRange = true;
          } else if (key > endKey) {
            return results;
          }
        }

        // 移动到下一个叶子节点
        currentLeafPointer = leafNode.getLong(8);

        // 如果没有找到范围内的键且不是第一个叶子节点，可以提前结束
        if (!foundInRange && !results.isEmpty()) {
          break;
        }
      } finally {
        leafNode.release();
      }
    }

    return results;
  }

  public boolean delete(long key) throws IOException {
    long rootPointer = mappedBuffer.getLong(ROOT_POINTER_OFFSET);
    return deleteRecursive(rootPointer, key);
  }

  private boolean deleteRecursive(long nodePointer, long key) throws IOException {
    ByteBuf node = readNode(nodePointer);
    try {
      byte nodeType = node.getByte(0);

      if (nodeType == LEAF_NODE) {
        return deleteFromLeaf(nodePointer, node, key);
      } else {
        return deleteFromInternal(nodePointer, node, key);
      }
    } finally {
      node.release();
    }
  }

  private boolean deleteFromLeaf(long nodePointer, ByteBuf node, long key) throws IOException {
    int keyCount = node.getInt(4);
    int deletePos = -1;

    for (int i = 0; i < keyCount; i++) {
      if (getKeyFromLeaf(node, i) == key) {
        deletePos = i;
        break;
      }
    }

    if (deletePos == -1) {
      return false;
    }

    // 移动后续键值对
    for (int i = deletePos; i < keyCount - 1; i++) {
      long nextKey = getKeyFromLeaf(node, i + 1);
      String nextValue = getValueFromLeaf(node, i + 1);

      setKeyInLeaf(node, i, nextKey);
      setValueInLeaf(node, i, nextValue);
    }

    node.setInt(4, keyCount - 1);
    writeNode(nodePointer, node);

    // 检查是否需要合并
    if (keyCount - 1 < (order - 1) / 2) {
      handleLeafUnderflow(nodePointer, node);
    }

    return true;
  }

  private boolean deleteFromInternal(long nodePointer, ByteBuf node, long key) throws IOException {
    int keyCount = node.getInt(4);
    int childIndex = findChildIndex(node, key, keyCount);
    long childPointer = getPointerFromInternal(node, childIndex);

    boolean deleted = deleteRecursive(childPointer, key);

    if (!deleted) {
      return false;
    }

    // 检查子节点是否需要合并
    ByteBuf childNode = readNode(childPointer);
    try {
      int childKeyCount = childNode.getInt(4);

      if (childKeyCount < (order - 1) / 2) {
        handleInternalUnderflow(nodePointer, node, childPointer, childNode, childIndex);
      }
    } finally {
      childNode.release();
    }

    return true;
  }

  private void handleLeafUnderflow(long nodePointer, ByteBuf node) throws IOException {
    // 实现叶子节点下溢处理（借键或合并）
    long prevPointer = node.getLong(16);
    long nextPointer = node.getLong(8);
    int keyCount = node.getInt(4);

    // 尝试从左兄弟借键
    if (prevPointer != -1) {
      ByteBuf prevNode = readNode(prevPointer);
      try {
        int prevKeyCount = prevNode.getInt(4);

        if (prevKeyCount > (order - 1) / 2) {
          // 借一个键值对
          long borrowedKey = getKeyFromLeaf(prevNode, prevKeyCount - 1);
          String borrowedValue = getValueFromLeaf(prevNode, prevKeyCount - 1);

          // 从原节点删除
          prevNode.setInt(4, prevKeyCount - 1);

          // 插入到当前节点
          insertKeyValueIntoLeaf(node, borrowedKey, borrowedValue, 0, keyCount);
          node.setInt(4, keyCount + 1);

          writeNode(prevPointer, prevNode);
          writeNode(nodePointer, node);
          return;
        }
      } finally {
        prevNode.release();
      }
    }

    // 尝试从右兄弟借键
    if (nextPointer != -1) {
      ByteBuf nextNode = readNode(nextPointer);
      try {
        int nextKeyCount = nextNode.getInt(4);

        if (nextKeyCount > (order - 1) / 2) {
          // 借一个键值对
          long borrowedKey = getKeyFromLeaf(nextNode, 0);
          String borrowedValue = getValueFromLeaf(nextNode, 0);

          // 从右兄弟删除第一个
          for (int i = 0; i < nextKeyCount - 1; i++) {
            long nextKey = getKeyFromLeaf(nextNode, i + 1);
            String nextValue = getValueFromLeaf(nextNode, i + 1);
            setKeyInLeaf(nextNode, i, nextKey);
            setValueInLeaf(nextNode, i, nextValue);
          }
          nextNode.setInt(4, nextKeyCount - 1);

          // 插入到当前节点
          insertKeyValueIntoLeaf(node, borrowedKey, borrowedValue, keyCount, keyCount);
          node.setInt(4, keyCount + 1);

          writeNode(nextPointer, nextNode);
          writeNode(nodePointer, node);
          return;
        }
      } finally {
        nextNode.release();
      }
    }

    // 需要合并节点
    if (prevPointer != -1) {
      mergeLeafNodes(prevPointer, nodePointer);
    } else if (nextPointer != -1) {
      mergeLeafNodes(nodePointer, nextPointer);
    }
  }

  private void mergeLeafNodes(long leftPointer, long rightPointer) throws IOException {
    ByteBuf leftNode = readNode(leftPointer);
    ByteBuf rightNode = readNode(rightPointer);

    try {
      int leftKeyCount = leftNode.getInt(4);
      int rightKeyCount = rightNode.getInt(4);

      // 合并键值对到左节点
      for (int i = 0; i < rightKeyCount; i++) {
        long key = getKeyFromLeaf(rightNode, i);
        String value = getValueFromLeaf(rightNode, i);
        setKeyInLeaf(leftNode, leftKeyCount + i, key);
        setValueInLeaf(leftNode, leftKeyCount + i, value);
      }

      leftNode.setInt(4, leftKeyCount + rightKeyCount);
      leftNode.setLong(8, rightNode.getLong(8)); // 更新next指针

      writeNode(leftPointer, leftNode);

      // 更新右节点的next节点的prev指针
      long rightNextPointer = rightNode.getLong(8);
      if (rightNextPointer != -1) {
        ByteBuf rightNextNode = readNode(rightNextPointer);
        try {
          rightNextNode.setLong(16, leftPointer);
          writeNode(rightNextPointer, rightNextNode);
        } finally {
          rightNextNode.release();
        }
      }

    } finally {
      leftNode.release();
      rightNode.release();
    }
  }

  private void handleInternalUnderflow(long parentPointer, ByteBuf parentNode,
      long childPointer, ByteBuf childNode, int childIndex) throws IOException {
    // 实现内部节点下溢处理
    // 这里简化实现，实际需要处理借键和合并
  }

  // ========== 辅助方法 ==========

  private int findInsertPositionInLeaf(ByteBuf node, long key, int keyCount) {
    for (int i = 0; i < keyCount; i++) {
      if (key < getKeyFromLeaf(node, i)) {
        return i;
      }
    }
    return keyCount;
  }

  private void insertKeyValueIntoLeaf(ByteBuf node, long key, String value, int position, int keyCount) {
    // 移动现有键值对
    for (int i = keyCount; i > position; i--) {
      long existingKey = getKeyFromLeaf(node, i - 1);
      String existingValue = getValueFromLeaf(node, i - 1);

      setKeyInLeaf(node, i, existingKey);
      setValueInLeaf(node, i, existingValue);
    }

    // 插入新键值对
    setKeyInLeaf(node, position, key);
    setValueInLeaf(node, position, value);
  }

  private void insertKeyPointerIntoInternal(ByteBuf node, long key, long pointer, int position, int keyCount) {
    // 移动现有键和指针
    for (int i = keyCount; i > position; i--) {
      long existingKey = getKeyFromInternal(node, i - 1);
      setKeyInInternal(node, i, existingKey);
    }

    for (int i = keyCount + 1; i > position + 1; i--) {
      long existingPointer = getPointerFromInternal(node, i - 1);
      setPointerInInternal(node, i, existingPointer);
    }

    // 插入新键和指针
    setKeyInInternal(node, position, key);
    setPointerInInternal(node, position + 1, pointer);
  }

  // ========== 节点读写方法 ==========

  private ByteBuf readNode(long pointer) {
    int position = (int) pointer;
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(PAGE_SIZE);

    byte[] data = new byte[PAGE_SIZE];
    ByteBuffer slice = mappedBuffer.slice();
    slice.position(position);
    slice.get(data);

    buf.writeBytes(data);
    return buf;
  }

  private void writeNode(long pointer, ByteBuf node) {
    int position = (int) pointer;
    node.readerIndex(0);

    byte[] data = new byte[PAGE_SIZE];
    node.readBytes(data);

    ByteBuffer slice = mappedBuffer.slice();
    slice.position(position);
    slice.put(data);

    mappedBuffer.force();
  }

  // ========== 节点数据访问方法 ==========

  private int getLeafKeyOffset(int index) {
    return NODE_HEADER_SIZE + index * KEY_SIZE;
  }

  private int getLeafValueOffset(int index) {
    return NODE_HEADER_SIZE + maxKeys * KEY_SIZE + index * 256; // 假设值最大256字节
  }

  private int getInternalKeyOffset(int index) {
    return NODE_HEADER_SIZE + index * KEY_SIZE;
  }

  private int getInternalPointerOffset(int index) {
    return NODE_HEADER_SIZE + maxKeys * KEY_SIZE + index * POINTER_SIZE;
  }

  private long getKeyFromLeaf(ByteBuf node, int index) {
    return node.getLong(getLeafKeyOffset(index));
  }

  private void setKeyInLeaf(ByteBuf node, int index, long key) {
    node.setLong(getLeafKeyOffset(index), key);
  }

  private String getValueFromLeaf(ByteBuf node, int index) {
    int offset = getLeafValueOffset(index);
    int length = node.getInt(offset);
    byte[] bytes = new byte[length];
    node.getBytes(offset + 4, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private void setValueInLeaf(ByteBuf node, int index, String value) {
    int offset = getLeafValueOffset(index);
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    node.setInt(offset, bytes.length);
    node.setBytes(offset + 4, bytes);
  }

  private long getKeyFromInternal(ByteBuf node, int index) {
    return node.getLong(getInternalKeyOffset(index));
  }

  private void setKeyInInternal(ByteBuf node, int index, long key) {
    node.setLong(getInternalKeyOffset(index), key);
  }

  private long getPointerFromInternal(ByteBuf node, int index) {
    return node.getLong(getInternalPointerOffset(index));
  }

  private void setPointerInInternal(ByteBuf node, int index, long pointer) {
    node.setLong(getInternalPointerOffset(index), pointer);
  }

  // ========== 清理方法 ==========

  public void close() throws IOException {
    // 更新next page id
    mappedBuffer.putLong(NEXT_PAGE_OFFSET, nextPageId.get());
    mappedBuffer.force();

    unmap(mappedBuffer);
    channel.close();
    raf.close();
  }

  @SuppressWarnings("unchecked")
  private void unmap(MappedByteBuffer buffer) {
    try {
      Method cleanerMethod = buffer.getClass().getMethod("cleaner");
      cleanerMethod.setAccessible(true);
      Object cleaner = cleanerMethod.invoke(buffer);
      Method cleanMethod = cleaner.getClass().getMethod("clean");
      cleanMethod.setAccessible(true);
      cleanMethod.invoke(cleaner);
    } catch (Exception e) {
      // 忽略清理错误
    }
  }

  // ========== 内部类 ==========

  private static class InsertResult {

    final long newChildKey;
    final long newChildPointer;

    InsertResult(long newChildKey, long newChildPointer) {
      this.newChildKey = newChildKey;
      this.newChildPointer = newChildPointer;
    }
  }

  // ========== 工具方法 ==========

  public void printTree() throws IOException {
    System.out.println("B+ Tree Structure:");
    long rootPointer = mappedBuffer.getLong(ROOT_POINTER_OFFSET);
    printNode(rootPointer, 0);
  }

  private void printNode(long nodePointer, int level) throws IOException {
    StringBuilder indent = new StringBuilder();
    for (int i = 0; i < level; i++) {
      indent.append("  ");
    }

    ByteBuf node = readNode(nodePointer);
    try {
      byte nodeType = node.getByte(0);
      int keyCount = node.getInt(4);

      if (nodeType == LEAF_NODE) {
        System.out.print(indent + "Leaf [" + keyCount + "]: ");
        for (int i = 0; i < keyCount; i++) {
          System.out.print(getKeyFromLeaf(node, i) + " ");
        }
        System.out.println();
      } else {
        System.out.print(indent + "Internal [" + keyCount + "]: ");
        for (int i = 0; i < keyCount; i++) {
          System.out.print(getKeyFromInternal(node, i) + " ");
        }
        System.out.println();

        // 递归打印子节点
        for (int i = 0; i <= keyCount; i++) {
          long childPointer = getPointerFromInternal(node, i);
          if (childPointer != -1) {
            printNode(childPointer, level + 1);
          }
        }
      }
    } finally {
      node.release();
    }
  }

  public TreeStats getStats() throws IOException {
    TreeStats stats = new TreeStats();
    long rootPointer = mappedBuffer.getLong(ROOT_POINTER_OFFSET);
    collectStats(rootPointer, stats, 0);
    return stats;
  }

  private void collectStats(long nodePointer, TreeStats stats, int depth) throws IOException {
    ByteBuf node = readNode(nodePointer);
    try {
      stats.totalNodes++;
      stats.depth = Math.max(stats.depth, depth);

      byte nodeType = node.getByte(0);
      int keyCount = node.getInt(4);

      if (nodeType == LEAF_NODE) {
        stats.leafNodes++;
        stats.totalKeys += keyCount;
      } else {
        stats.internalNodes++;
        stats.totalKeys += keyCount;

        // 递归统计子节点
        for (int i = 0; i <= keyCount; i++) {
          long childPointer = getPointerFromInternal(node, i);
          if (childPointer != -1) {
            collectStats(childPointer, stats, depth + 1);
          }
        }
      }
    } finally {
      node.release();
    }
  }

  public static class TreeStats {

    public int totalNodes;
    public int leafNodes;
    public int internalNodes;
    public int totalKeys;
    public int depth;

    @Override
    public String toString() {
      return String.format("TreeStats{totalNodes=%d, leafNodes=%d, internalNodes=%d, totalKeys=%d, depth=%d}",
          totalNodes, leafNodes, internalNodes, totalKeys, depth);
    }
  }
}