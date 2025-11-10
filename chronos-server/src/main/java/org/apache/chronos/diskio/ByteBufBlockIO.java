package org.apache.chronos.diskio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ByteBufBlockIO {

  private final FileChannel fileChannel;
  private final ByteBufAllocator allocator;
  private final int blockSize;
  private final boolean useDirectBuffer;

  // 统计信息
  private final AtomicLong readCount = new AtomicLong();
  private final AtomicLong writeCount = new AtomicLong();
  private final AtomicLong totalBytesRead = new AtomicLong();
  private final AtomicLong totalBytesWritten = new AtomicLong();

  public ByteBufBlockIO(String filePath, int blockSize, boolean useDirectBuffer) throws IOException {
    this.blockSize = blockSize;
    this.useDirectBuffer = useDirectBuffer;
    this.allocator = useDirectBuffer ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
    this.fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
  }

  /**
   * 块读取 - 使用ByteBuf避免内存拷贝
   */
  public ByteBuf readBlock(long blockIndex) throws IOException {
    long position = blockIndex * blockSize;
    long fileSize = fileChannel.size();

    if (position >= fileSize) {
      return Unpooled.EMPTY_BUFFER;
    }

    int bytesToRead = (int) Math.min(blockSize, fileSize - position);
    ByteBuf buffer = allocator.buffer(bytesToRead, bytesToRead);

    try {
      int bytesRead = readToByteBuf(buffer, position, bytesToRead);
      if (bytesRead != bytesToRead) {
        buffer.release();
        throw new IOException("Failed to read complete block");
      }

      readCount.incrementAndGet();
      totalBytesRead.addAndGet(bytesRead);
      return buffer;

    } catch (Exception e) {
      buffer.release();
      throw new IOException("Failed to read block " + blockIndex, e);
    }
  }

  /**
   * 高效读取到ByteBuf，避免中间拷贝
   */
  private int readToByteBuf(ByteBuf buffer, long position, int length) throws IOException {
    int totalRead = 0;

    while (totalRead < length) {
      // 获取ByteBuf的内部ByteBuffer视图
      ByteBuffer byteBuffer = buffer.internalNioBuffer(buffer.writerIndex(), length - totalRead);

      int bytesRead = fileChannel.read(byteBuffer, position + totalRead);
      if (bytesRead == -1) {
        break;
      }

      // 更新ByteBuf的写指针
      buffer.writerIndex(buffer.writerIndex() + bytesRead);
      totalRead += bytesRead;
    }

    return totalRead;
  }

  /**
   * 块写入 - 批量写入提高性能
   */
  public void writeBlock(long blockIndex, ByteBuf data) throws IOException {
    long position = blockIndex * blockSize;
    int dataSize = data.readableBytes();

    if (dataSize > blockSize) {
      throw new IllegalArgumentException("Data size exceeds block size");
    }

    try {
      int bytesWritten = writeFromByteBuf(data, position);
      if (bytesWritten != dataSize) {
        throw new IOException("Failed to write complete block");
      }

      writeCount.incrementAndGet();
      totalBytesWritten.addAndGet(bytesWritten);

    } finally {
      data.release(); // 重要：释放ByteBuf
    }
  }

  /**
   * 从ByteBuf高效写入，避免中间拷贝
   */
  private int writeFromByteBuf(ByteBuf buffer, long position) throws IOException {
    int totalWritten = 0;
    int length = buffer.readableBytes();

    while (totalWritten < length) {
      // 获取可读数据的ByteBuffer视图
      ByteBuffer byteBuffer = buffer.internalNioBuffer(buffer.readerIndex() + totalWritten,
          length - totalWritten);

      int bytesWritten = fileChannel.write(byteBuffer, position + totalWritten);
      if (bytesWritten == -1) {
        break;
      }

      totalWritten += bytesWritten;
    }

    return totalWritten;
  }

  /**
   * 批量块读取
   */
  public List<ByteBuf> readMultipleBlocks(List<Long> blockIndices) throws IOException {
    List<ByteBuf> results = new ArrayList<>(blockIndices.size());

    for (Long blockIndex : blockIndices) {
      ByteBuf block = readBlock(blockIndex);
      if (block != Unpooled.EMPTY_BUFFER) {
        results.add(block);
      }
    }

    return results;
  }

  /**
   * 批量块写入
   */
  public void writeMultipleBlocks(Map<Long, ByteBuf> blocks) throws IOException {
    for (Map.Entry<Long, ByteBuf> entry : blocks.entrySet()) {
      writeBlock(entry.getKey(), entry.getValue());
    }
  }

  /**
   * 预读取多个块（顺序读取优化）
   */
  public List<ByteBuf> readSequentialBlocks(long startBlock, int count) throws IOException {
    List<ByteBuf> blocks = new ArrayList<>(count);

    for (long i = startBlock; i < startBlock + count; i++) {
      ByteBuf block = readBlock(i);
      if (block == Unpooled.EMPTY_BUFFER) {
        break;
      }
      blocks.add(block);
    }

    return blocks;
  }

  public void close() throws IOException {
    fileChannel.close();
    printStats();
  }

  private void printStats() {
    System.out.println("=== NettyBlockIO 统计 ===");
    System.out.println("读取次数: " + readCount.get());
    System.out.println("写入次数: " + writeCount.get());
    System.out.println("读取字节: " + totalBytesRead.get());
    System.out.println("写入字节: " + totalBytesWritten.get());
  }
}
