package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.apache.chronos.common.FileUtil;
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
  private int wrtIdx;

  public BlockChannel(String filePath) throws Exception {
    File file = new File(filePath);
    if (!file.exists()) {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      fileChannel.truncate(PAGE_SIZE * PAGE_NUM);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE * PAGE_NUM);
      mappedByteBuffer.flip();
      byteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
      updateBlockHeader(BLOCK_HEADER_SIZE);
      persist();
    } else {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size());
      mappedByteBuffer.flip();
      byteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
      wrtIdx = byteBuf.getInt(4);
    }
  }

  private void updateBlockHeader(int wrtIdx) {
    this.wrtIdx = wrtIdx;
    byteBuf.writerIndex(0);
    byteBuf.writeInt(MAGIC_VALUE);
    byteBuf.writeInt(wrtIdx);
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
  }

  private void persist() throws IOException {
    mappedByteBuffer.force();
    fileChannel.force(true);
  }

  public long getFileSize() throws IOException {
    return fileChannel.size();
  }

  public int addRoaringBitmap(int tagIndex, RoaringBitmap roaringBitmap) throws Exception {
    int needSize = roaringBitmap.serializedSizeInBytes();
    while (getAvailableFileSize() < needSize + ENTRY_HEADER_SIZE + PAGE_HEADER_SIZE) {
      expandChannelFile();
    }
    // The channel file has enough space to hold the data
    int currentPageAvailableSize = PAGE_SIZE - getUsedPageSize(wrtIdx) - PAGE_HEADER_SIZE;
    int result = wrtIdx;
    int pageIndex = getPageOffset(wrtIdx);
    byteBuf.writerIndex(wrtIdx);

    if (pageIndex == wrtIdx) {
      // Page Header: page size
      byteBuf.writeInt(0);
      // Page Header: reserve
      byteBuf.skipBytes(PAGE_HEADER_SIZE - 4);
    }
    // single page
    if (needSize + ENTRY_HEADER_SIZE <= currentPageAvailableSize) {
      // update page header
      addUsedPageSize(result, needSize + ENTRY_HEADER_SIZE);
      // entry header 32 bytes
      byteBuf.writeByte(1);
      byteBuf.writeMedium(needSize);
      // next entry offset
      byteBuf.writeInt(0);
      // tag index
      byteBuf.writeInt(tagIndex);
      byteBuf.skipBytes(ENTRY_HEADER_SIZE - 12);
      roaringBitmap.serialize(new ByteBufOutputStream(byteBuf));
      wrtIdx = byteBuf.writerIndex();
      updateBlockHeader(wrtIdx);
      persist();
    } else {
      CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
      try {
        addUsedPageSize(byteBuf.writerIndex(), needSize + ENTRY_HEADER_SIZE);
        byteBuf.writeByte(2);
        byteBuf.writeMedium(currentPageAvailableSize - ENTRY_HEADER_SIZE);
        // next entry offset
        byteBuf.writeInt(0);
        // tag index
        byteBuf.writeInt(tagIndex);
        byteBuf.skipBytes(ENTRY_HEADER_SIZE - 12);
        compositeByteBuf.addComponent(byteBuf.slice(byteBuf.writerIndex(), currentPageAvailableSize - ENTRY_HEADER_SIZE));
        byteBuf.skipBytes(currentPageAvailableSize - ENTRY_HEADER_SIZE);

        int leaveSize = needSize - (currentPageAvailableSize - ENTRY_HEADER_SIZE);
        while (leaveSize > 0) {
          // next page
          pageIndex = getPageOffset(byteBuf.writerIndex());
          if (pageIndex == wrtIdx) {
            // Page Header: page size
            byteBuf.writeInt(0);
            // Page Header: reserve
            byteBuf.skipBytes(PAGE_HEADER_SIZE - 4);
          }

          int writeSize = Math.min(leaveSize, PAGE_SIZE - PAGE_HEADER_SIZE - ENTRY_HEADER_SIZE);

          addUsedPageSize(byteBuf.writerIndex(), writeSize + ENTRY_HEADER_SIZE);
          byteBuf.writeByte(2);
          byteBuf.writeMedium(writeSize);
          // next entry offset
          byteBuf.writeInt(0);
          // tag index
          byteBuf.writeInt(tagIndex);
          byteBuf.skipBytes(ENTRY_HEADER_SIZE - 12);
          compositeByteBuf.addComponent(byteBuf.slice(byteBuf.writerIndex(), writeSize));
          byteBuf.skipBytes(writeSize);

          leaveSize = needSize - writeSize;
        }

        roaringBitmap.serialize(new ByteBufOutputStream(compositeByteBuf));
      } finally {
        ReferenceCountUtil.safeRelease(compositeByteBuf);
      }
    }
    return result;
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
    byteBuf.markReaderIndex();
    byteBuf.readerIndex(entryOffset);
    int flag = byteBuf.readUnsignedByte();
    int size = byteBuf.readUnsignedMedium();
    int nextEntryOffset = byteBuf.readInt();
    byteBuf.skipBytes(24);

    if (size <= 0 || flag < 1) {
      return null; // Entry is deleted
    }
    ByteBuf sliceByteBuf = byteBuf.slice(entryOffset + ENTRY_HEADER_SIZE, size);
    byteBuf.resetReaderIndex();

    RoaringBitmap bitmap = new RoaringBitmap();

    if (flag == 1) {
      // Single page entry
      bitmap.deserialize(new ByteBufInputStream(sliceByteBuf));
    } else if (flag == 2) {
      CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
      try {
        compositeByteBuf.addComponent(sliceByteBuf);
        while (flag == 2 && nextEntryOffset != 0) {
          byteBuf.markReaderIndex();
          byteBuf.readerIndex(nextEntryOffset);
          flag = byteBuf.readUnsignedByte();
          size = byteBuf.readUnsignedMedium();
          nextEntryOffset = byteBuf.readInt();
          byteBuf.skipBytes(24);
          compositeByteBuf.addComponent(byteBuf.slice(entryOffset + ENTRY_HEADER_SIZE, size));
          byteBuf.resetReaderIndex();
        }
        bitmap.deserialize(new ByteBufInputStream(compositeByteBuf));
      } finally {
        ReferenceCountUtil.safeRelease(compositeByteBuf);
      }
    }
    return bitmap;
  }

  private long getAvailableFileSize() throws Exception {
    return fileChannel.size() - wrtIdx;
  }

  private int getPageOffset(int offset) {
    return Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
  }

  private int getUsedPageSize(int offset) throws Exception {
    int pageIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
    return byteBuf.getInt(pageIndex);
  }

  protected void addUsedPageSize(int offset, int addSize) {
    int pageIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE) * PAGE_SIZE + BLOCK_HEADER_SIZE;
    int newSize = byteBuf.getInt(pageIndex) + addSize;
    byteBuf.setInt(pageIndex, newSize);
  }

  private int getPageEntryNumber(int offset) throws Exception {
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
      idx = idx + 8 + size;
    }
    return counter;
  }
}
