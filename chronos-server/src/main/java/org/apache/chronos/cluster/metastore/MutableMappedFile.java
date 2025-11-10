package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 可修改的内存映射方案
 */
public class MutableMappedFile {
  private final FileChannel fileChannel;
  private final MappedByteBuffer mappedBuffer;
  private final ByteBuf byteBuf;
  private final long fileSize;

  public MutableMappedFile(Path filePath, long fileSize) throws IOException {
    this.fileSize = fileSize;

    // 确保文件存在且有足够大小
    ensureFileSize(filePath, fileSize);

    this.fileChannel = FileChannel.open(filePath,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE);

    // 读写模式映射
    this.mappedBuffer = fileChannel.map(
        FileChannel.MapMode.READ_WRITE, 0, fileSize);

    // 包装为ByteBuf，注意这里使用wrappedBuffer不会拷贝数据
    this.byteBuf = Unpooled.wrappedBuffer(mappedBuffer);
  }

  private void ensureFileSize(Path filePath, long size) throws IOException {
    File file = filePath.toFile();
    if (!file.exists() || file.length() < size) {
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.setLength(size);
      }
    }
  }

  /**
   * 获取可修改的ByteBuf
   */
  public ByteBuf getWritableBuffer() {
    return byteBuf;
  }

  /**
   * 强制持久化到硬盘
   */
  public void persist() throws IOException {
    // 强制将修改刷到磁盘
    mappedBuffer.force();

    // 如果是重要数据，确保元数据也持久化
    fileChannel.force(true);
  }

  /**
   * 遍历并处理内容
   */
  public void traverseAndModify() throws IOException {
    ByteBuf buffer = getWritableBuffer();

    // 遍历修改示例
    for (int i = 0; i < buffer.capacity(); i++) {
      byte original = buffer.getByte(i);
      byte modified = (byte) (original + 1); // 示例修改
      buffer.setByte(i, modified);
    }

    // 立即持久化
    persist();
  }

  /**
   * 批量修改优化
   */
  public void batchModify(ByteProcessor modifier) throws IOException {
    ByteBuf buffer = getWritableBuffer();

    if (buffer.hasArray()) {
      // 使用数组批量操作，性能最好
      byte[] array = buffer.array();
      int offset = buffer.arrayOffset();

      for (int i = 0; i < buffer.capacity(); i++) {
        array[offset + i] = modifier.process(array[offset + i]);
      }
    } else {
      // 直接操作ByteBuf
      for (int i = 0; i < buffer.capacity(); i++) {
        byte modified = modifier.process(buffer.getByte(i));
        buffer.setByte(i, modified);
      }
    }

    persist();
  }

  public void close() throws IOException {
    if (byteBuf != null) {
      byteBuf.release();
    }
    if (fileChannel != null) {
      fileChannel.close();
    }
  }
}
