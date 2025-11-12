package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.apache.chronos.cluster.meta.Offset;
import org.apache.chronos.cluster.meta.serializer.OffsetSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 可修改的内存映射方案
 */
public class DiskOffsetIndex implements IOffsetIndexStore {

  private final static Logger log = LogManager.getLogger(DiskOffsetIndex.class);

  private static final int DEFAULT_PAGE_SIZE = 4096 * 16;
  private static final int FILE_HEADER_SIZE = 16;
  private final FileChannel fileChannel;
  private MappedByteBuffer mappedBuffer;
  private ByteBuf byteBuf;
  private volatile long fileSize;
  private volatile int maxMetaDataId;
  private volatile int metaDataVersion;
  private volatile int metaDataCounter;

  public DiskOffsetIndex(File file) throws IOException {
    init(file);
    this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    this.fileSize = fileChannel.size();
    this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    this.byteBuf = Unpooled.wrappedBuffer(mappedBuffer);
    this.metaDataVersion = byteBuf.getInt(4);
    this.maxMetaDataId = byteBuf.getInt(8);
    this.metaDataCounter = byteBuf.getInt(12);
  }

  private void init(File file) throws IOException {
    if (!file.exists()) {
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.setLength(DEFAULT_PAGE_SIZE);
        // write file header
        raf.seek(0);
        // magic
        raf.writeInt(0x19870712);
        // file version
        raf.writeInt(0);
        // maxMetaDataId
        raf.writeInt(0);
        // metaDataCounter
        raf.writeInt(0);
        raf.getFD().sync();
      }
    }
  }

  private void expandSpaceIfNecessary(int actualWrtIndex) throws IOException {
    long availableSpace = fileSize - actualWrtIndex;
    if (availableSpace < (DEFAULT_PAGE_SIZE >> 2)) {
      log.info("Meta index file space not enough, try to expand. availableSpace: {}", availableSpace);
      persist();
      // 2. 解除当前内存映射
      clean(mappedBuffer);
      byteBuf.release();
      // 3. 调整文件大小
      fileSize += DEFAULT_PAGE_SIZE;
      fileChannel.truncate(fileSize);
      // 4. 重新建立内存映射
      this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
      this.byteBuf = Unpooled.wrappedBuffer(mappedBuffer);
    }
  }

  private void clean(MappedByteBuffer buffer) {
    if (buffer == null) {
      return;
    }
    try {
      Method cleaner = buffer.getClass().getMethod("cleaner");
      cleaner.setAccessible(true);
      Object clean = cleaner.invoke(buffer);
      if (clean != null) {
        clean.getClass().getMethod("clean").invoke(clean);
      }
    } catch (Exception e) {
      log.error("Meta Index clean failed", e);
    }
  }

  private void persist() throws IOException {
    byteBuf.setInt(4, metaDataVersion);
    byteBuf.setInt(8, maxMetaDataId);
    byteBuf.setInt(12, metaDataCounter);
    // 强制将修改刷到磁盘
    mappedBuffer.force();
    // 如果是重要数据，确保元数据也持久化
    fileChannel.force(true);
  }

  public void close() throws IOException {
    if (byteBuf != null) {
      byteBuf.release();
    }
    if (mappedBuffer != null) {
      clean(mappedBuffer); // 清理内存映射
      mappedBuffer = null;
    }
    if (fileChannel != null) {
      fileChannel.close();
    }
  }

  @Override
  public synchronized void upsertOffset(int metaDataId, Offset offset) throws IOException {
    int wtx = (metaDataId - 1) * Offset.TOTAL_SIZE + FILE_HEADER_SIZE;
    expandSpaceIfNecessary(wtx + Offset.TOTAL_SIZE);
    if (maxMetaDataId < metaDataId) {
      this.maxMetaDataId = metaDataId;
    }
    int status = byteBuf.getUnsignedByte(wtx);
    if (status != Offset.STATUS_NORMAL) {
      this.metaDataCounter++;
    }
    this.metaDataVersion++;
    OffsetSerializer.INSTANCE.serialize(byteBuf.slice(wtx, Offset.TOTAL_SIZE).writerIndex(0), offset);
    persist();
  }

  @Override
  public synchronized Offset getOffset(int metaDataId) {
    if (maxMetaDataId < metaDataId) {
      return null;
    }
    int rdx = (metaDataId - 1) * Offset.TOTAL_SIZE + FILE_HEADER_SIZE;
    int status = byteBuf.getUnsignedByte(rdx);
    if (status == Offset.STATUS_NORMAL) {
      return OffsetSerializer.INSTANCE.deserialize(byteBuf.slice(rdx, Offset.TOTAL_SIZE).readerIndex(0));
    } else {
      return null;
    }
  }

  @Override
  public synchronized void removeOffset(int metaDataId) throws IOException {
    if (maxMetaDataId < metaDataId) {
      return;
    }
    this.metaDataVersion++;
    int idx = (metaDataId - 1) * Offset.TOTAL_SIZE + FILE_HEADER_SIZE;
    int status = byteBuf.getUnsignedByte(idx);
    if (status == Offset.STATUS_NORMAL) {
      this.metaDataCounter--;
    }
    byteBuf.setByte(idx, Offset.STATUS_DELETED);
    persist();
  }

  @Override
  public synchronized int getMetaDataVersion() {
    return this.metaDataVersion;
  }

  @Override
  public int getSize() {
    return this.metaDataCounter;
  }
}
