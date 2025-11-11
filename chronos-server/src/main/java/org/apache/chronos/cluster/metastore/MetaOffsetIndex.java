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
import org.apache.chronos.cluster.meta.Offset;
import org.apache.chronos.cluster.meta.serializer.OffsetSerializer;

/**
 * 可修改的内存映射方案
 */
public class MetaOffsetIndex {

  private static final int DEFAULT_PAGE_SIZE = 4096 * 16;

  private final FileChannel fileChannel;
  private final MappedByteBuffer mappedBuffer;
  private final ByteBuf byteBuf;

  public MetaOffsetIndex(Path filePath) throws IOException {
    init(filePath);
    this.fileChannel = FileChannel.open(filePath,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE);
    long fileSize = fileChannel.size();
    // 读写模式映射
    this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    // 包装为ByteBuf，注意这里使用wrappedBuffer不会拷贝数据
    this.byteBuf = Unpooled.wrappedBuffer(mappedBuffer);
  }

  private void init(Path filePath) throws IOException {
    File file = filePath.toFile();
    if (!file.exists()) {
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.setLength(DEFAULT_PAGE_SIZE);
        // write file header
        raf.seek(0);
        // magic
        raf.writeInt(0x870712);
        // file version
        raf.writeInt(0x00);
        // writer index
        raf.writeLong(16);
        raf.getFD().sync();
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

  public void close() throws IOException {
    if (byteBuf != null) {
      byteBuf.release();
    }
    if (fileChannel != null) {
      fileChannel.close();
    }
  }

  public void addEntry(Offset offset) throws IOException {
    OffsetSerializer.INSTANCE.serialize(byteBuf, offset);
    persist();
  }

  public Offset getOffset(int metaDataId) throws IOException {
    return null;
  }
}
