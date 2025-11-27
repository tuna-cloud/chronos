package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.chronos.common.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

/**
 * Page size is 128 KB.
 * <p>
 * |----------- block header(32 bytes)-------|------ page1 ------|------- page2 ---------|---------- pageN ----------|
 * <p>
 * |--- magic value ---|--- writer index ---|---
 * <p>
 * |--- 4 bytes -------|--- 4 bytes --------|--- -----------------------------------------|-------------------|-----------------------|
 * <p>
 * |----------- page header(16 bytes) --------------|
 * <p>
 * |--- used page size ---|----------|---
 * <p>
 * |--- 4 bytes ----------|-----|----------|---
 * <p>
 * |----------- entry header(32 bytes) --------------|
 * <p>
 * |--- flag ---|--- entry size --|--- next page offset ---|--- tags index offset ---|
 * <p>
 * |--- 1 bytes |--- 3 bytes -----|--------- 4 bytes ------|--- 4 bytes -------------|
 * <p>
 * flag 0： no entry, 1: single page, flag 2: continue page.
 * <p>
 */
public class BlockChannel {

  private static final Logger log = LogManager.getLogger(BlockChannel.class);

  // bytes
  private static final int BLOCK_HEADER_SIZE = 32;
  private static final int PAGE_HEADER_SIZE = 16;
  private static final int ENTRY_HEADER_SIZE = 32;
  private static final int MAGIC_VALUE = 0x870712;
  // 128 KB
  private static final int PAGE_SIZE = 128 * 1024;
  // page number
  private static final int PAGE_NUM = 256;
  private final FileChannel fileChannel;
  private MappedByteBuffer mappedByteBuffer;
  private ByteBuf byteBuf;
  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  public BlockChannel(String filePath) throws Exception {
    File file = new File(filePath);
    if (!file.exists()) {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      fileChannel.truncate(PAGE_SIZE * PAGE_NUM);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE * PAGE_NUM);
      byteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
      byteBuf.readerIndex(0);
      updateBlockHeader(BLOCK_HEADER_SIZE);
      byteBuf.writerIndex(BLOCK_HEADER_SIZE);
      persist();
    } else {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size());
      byteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
      byteBuf.readerIndex(0);
      byteBuf.writerIndex(getWrtIdx());
    }
  }

  private void updateBlockHeader(int wrtIdx) {
    byteBuf.setInt(0, MAGIC_VALUE);
    byteBuf.setInt(4, wrtIdx);
  }

  private int getWrtIdx() {
    return byteBuf.getInt(4);
  }

  protected void expandChannelFile() throws Exception {
    if (fileChannel.size() == Integer.MAX_VALUE) {
      throw new MemMapSpaceExceedException();
    }
    long newSize = fileChannel.size() * 2;
    if (newSize > Integer.MAX_VALUE) {
      newSize = Integer.MAX_VALUE;
    }
    persist();
    FileUtil.clean(mappedByteBuffer);
    byteBuf.release();
    fileChannel.truncate(newSize);
    this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
    this.byteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
    byteBuf.readerIndex(0);
    byteBuf.writerIndex(getWrtIdx());
  }

  private void persist() throws IOException {
    mappedByteBuffer.force();
    fileChannel.force(true);
  }

  public long getFileSize() throws IOException {
    return fileChannel.size();
  }

  public int addRoaringBitmap(int tagIndex, RoaringBitmap roaringBitmap) throws Exception {
    try {
      writeLock.lock();
      int needSize = roaringBitmap.serializedSizeInBytes();
      while (getAvailableFileSize() < needSize + ENTRY_HEADER_SIZE + PAGE_HEADER_SIZE) {
        expandChannelFile();
      }
      // The channel file has enough space to hold the data
      int currentPageAvailableSize = PAGE_SIZE - getUsedPageSize(byteBuf.writerIndex()) - PAGE_HEADER_SIZE;
      int pageIndex = getPageOffset(byteBuf.writerIndex());

      if (pageIndex == byteBuf.writerIndex()) {
        // Page Header: page size
        byteBuf.writeInt(0);
        // Page Header: reserve
        byteBuf.writerIndex(byteBuf.writerIndex() + PAGE_HEADER_SIZE - 4);
      }
      int result = byteBuf.writerIndex();
      // single page
      if (needSize + ENTRY_HEADER_SIZE <= currentPageAvailableSize) {
        log.info("write entry offset: {}, size: {}", result, needSize);
        // update page header
        addUsedPageSize(result, needSize + ENTRY_HEADER_SIZE);
        // entry header 32 bytes
        byteBuf.writeByte(1);
        byteBuf.writeMedium(needSize);
        // next entry offset
        byteBuf.writeInt(0);
        // tag index
        byteBuf.writeInt(tagIndex);
        byteBuf.writerIndex(byteBuf.writerIndex() + ENTRY_HEADER_SIZE - 12);
        roaringBitmap.serialize(new ByteBufOutputStream(byteBuf));
        updateBlockHeader(byteBuf.writerIndex());
        persist();
      } else {
        log.info("write entry offset: {}, size: {}", result, currentPageAvailableSize - ENTRY_HEADER_SIZE);
        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        addUsedPageSize(byteBuf.writerIndex(), currentPageAvailableSize);
        byteBuf.writeByte(2);
        byteBuf.writeMedium(currentPageAvailableSize - ENTRY_HEADER_SIZE);
        // next entry offset
        byteBuf.writeInt(getNextPageOffset(byteBuf.writerIndex()) + PAGE_HEADER_SIZE);
        // tag index
        byteBuf.writeInt(tagIndex);
        byteBuf.writerIndex(byteBuf.writerIndex() + ENTRY_HEADER_SIZE - 12);
        compositeByteBuf.addComponent(byteBuf.slice(byteBuf.writerIndex(), currentPageAvailableSize - ENTRY_HEADER_SIZE));
        byteBuf.writerIndex(byteBuf.writerIndex() + currentPageAvailableSize - ENTRY_HEADER_SIZE);

        int leaveSize = needSize - (currentPageAvailableSize - ENTRY_HEADER_SIZE);
        while (leaveSize > 0) {
          // next page
          pageIndex = getPageOffset(byteBuf.writerIndex());
          if (pageIndex == byteBuf.writerIndex()) {
            // Page Header: page size
            byteBuf.writeInt(0);
            // Page Header: reserve
            byteBuf.writerIndex(byteBuf.writerIndex() + PAGE_HEADER_SIZE - 4);
          }

          int writeSize = Math.min(leaveSize, PAGE_SIZE - PAGE_HEADER_SIZE - ENTRY_HEADER_SIZE);
          leaveSize = leaveSize - writeSize;
          log.info("write entry offset: {}, size: {}", byteBuf.writerIndex(), writeSize);

          addUsedPageSize(byteBuf.writerIndex(), writeSize + ENTRY_HEADER_SIZE);
          byteBuf.writeByte(2);
          byteBuf.writeMedium(writeSize);
          // next entry offset
          if (leaveSize > 0) {
            byteBuf.writeInt(getNextPageOffset(byteBuf.writerIndex()) + PAGE_HEADER_SIZE);
          } else {
            byteBuf.writeInt(0);
          }
          // tag index
          byteBuf.writeInt(tagIndex);
          byteBuf.writerIndex(byteBuf.writerIndex() + ENTRY_HEADER_SIZE - 12);
          compositeByteBuf.addComponent(byteBuf.slice(byteBuf.writerIndex(), writeSize));
          byteBuf.writerIndex(byteBuf.writerIndex() + writeSize);
        }
        roaringBitmap.serialize(new ByteBufOutputStream(compositeByteBuf));
        updateBlockHeader(byteBuf.writerIndex());
        persist();
      }
      return result;
    } finally {
      writeLock.unlock();
    }
  }

  public void updateRoaringBitmap(int offset, RoaringBitmap roaringBitmap) {
    // todo
  }

  public void deleteRoaringBitmap(int offset) {

    // todo
  }

  public RoaringBitmap getRoaringBitmap(int entryOffset) throws IOException {
    if (entryOffset <= 0) {
      return null;
    }
    try {
      readLock.lock();
      int flag = byteBuf.getUnsignedByte(entryOffset);
      int size = byteBuf.getUnsignedMedium(entryOffset + 1);
      int nextEntryOffset = byteBuf.getInt(entryOffset + 4);

      if (size <= 0 || flag < 1) {
        return null; // Entry is deleted
      }
      ByteBuf sliceByteBuf = byteBuf.slice(entryOffset + ENTRY_HEADER_SIZE, size);

      RoaringBitmap bitmap = new RoaringBitmap();

      if (flag == 1) {
        // Single page entry
        log.info("read entry offset: {}, size: {}", entryOffset, size);
        bitmap.deserialize(new ByteBufInputStream(sliceByteBuf));
      } else if (flag == 2) {
        log.info("read entry offset: {}, size: {}", entryOffset, size);
        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        compositeByteBuf.addComponent(sliceByteBuf);
        while (flag == 2 && nextEntryOffset != 0) {
          flag = byteBuf.getUnsignedByte(nextEntryOffset);
          size = byteBuf.getUnsignedMedium(nextEntryOffset + 1);
          log.info("read entry offset: {}, size: {}", nextEntryOffset, size);
          compositeByteBuf.addComponent(byteBuf.slice(nextEntryOffset + ENTRY_HEADER_SIZE, size));
          nextEntryOffset = byteBuf.getInt(nextEntryOffset + 4);
        }
        compositeByteBuf.writerIndex(compositeByteBuf.capacity());
        bitmap.deserialize(new ByteBufInputStream(compositeByteBuf));
      }
      return bitmap;
    } finally {
      readLock.unlock();
    }
  }

  protected long getAvailableFileSize() throws Exception {
    return fileChannel.size() - byteBuf.writerIndex();
  }

  protected int getPageOffset(int offset) {
    return Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
  }

  protected int getNextPageOffset(int offset) {
    return getPageOffset(offset) + PAGE_SIZE;
  }

  protected int getUsedPageSize(int offset) {
    int pageIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
    return byteBuf.getInt(pageIndex);
  }

  protected void addUsedPageSize(int offset, int addSize) {
    int pageIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
    int newSize = byteBuf.getInt(pageIndex) + addSize;
    byteBuf.setInt(pageIndex, newSize);
  }

  protected int getPageEntryNumber(int offset) {
    int pageStartIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
    if (byteBuf.getInt(pageStartIndex) == 0) {
      return 0;
    }
    int pageEndIndex = pageStartIndex + PAGE_SIZE;
    int counter = 0;
    int idx = pageStartIndex + PAGE_HEADER_SIZE;
    while (idx < pageEndIndex) {
      int size = byteBuf.getMedium(idx + 1);
      if (size <= 0) {
        break;
      }
      counter++;
      idx = idx + ENTRY_HEADER_SIZE + size;
    }
    return counter;
  }

  protected void prettyDebug() throws IOException {
    int idx = BLOCK_HEADER_SIZE;
    log.info("wrtIdx: {}", byteBuf.getInt(4));
    while (idx < getFileSize()) {
      log.info("Page idx: {}, page available size: {}, entry num: {}", (idx - BLOCK_HEADER_SIZE) / PAGE_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE - byteBuf.getInt(idx), getPageEntryNumber(idx));
      idx += PAGE_SIZE;
    }
  }
}
