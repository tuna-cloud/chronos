package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
 * |----------- page header(32 bytes) --------------|
 * <p>
 * |--- used page size ---|----------|---
 * <p>
 * |--- 4 bytes ----------|-----|----------|---
 * <p>
 * |----------- entry header(32 bytes) --------------|
 * <p>
 * |--- flag ---|--- entry size --|--- next page offset ---|
 * <p>
 * |--- 1 bytes |--- 3 bytes -----|--------- 4 bytes ------|
 * <p>
 *  flag 0： single page, flag 1: continue page.
 * <p>
 */
public class BlockChannel {

  // bytes
  private static final int BLOCK_HEADER_SIZE = 32;
  private static final int PAGE_HEADER_SIZE = 16;
  private static final int MAGIC_VALUE = 0x870712;
  // 16 KB
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
    } else {
      fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size());
      mappedByteBuffer.flip();
      byteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
    }
  }

  private void updateBlockHeader(int wrtIdx) throws IOException {
    this.wrtIdx = wrtIdx;
    byteBuf.writerIndex(0);
    byteBuf.writeInt(MAGIC_VALUE);
    byteBuf.writeInt(wrtIdx);
    persist();
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

  public int addRoaringBitmap(RoaringBitmap roaringBitmap) throws Exception {
    int needSize = roaringBitmap.serializedSizeInBytes();
    while (getAvailableFileSize() < needSize) {
      expandChannelFile();
    }
    // The channel file has enough space to hold the data

    return 0;
  }

  public void updateRoaringBitmap(int offset, RoaringBitmap roaringBitmap) {

  }

  public void deleteRoaringBitmap(int offset) {

  }

  public RoaringBitmap getRoaringBitmap(int offset) {
    return null;
  }

  private long getAvailableFileSize() throws Exception {
    return fileChannel.size() - wrtIdx;
  }

  private int getUsedPageSize(int offset) throws Exception {
    int pageIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE);
    return byteBuf.getInt(pageIndex);
  }

  private int getPageEntryNumber(int offset) throws Exception {
    int pageStartIndex = Math.floorDiv(offset - BLOCK_HEADER_SIZE, PAGE_SIZE);
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
      counter ++;
      idx = idx + 8 + size;
    }
    return counter;
  }
}
