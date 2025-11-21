package org.apache.chronos.cluster.metastore;

import com.apache.chronos.protocol.codec.CodecUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.chronos.common.CfgUtil;
import org.apache.chronos.common.ChronosConfig;
import org.apache.commons.codec.digest.XXHash32;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

public class TagsIndexManager {

  private final String indexFilePath;
  private final int indexMaxCapacity;
  private final IOffsetIndexStore offsetIndexStore;
  /**
   * index file name format： tag_index_000 tag_index_001 ...
   */
  private static final String TAG_INDEX_FILE = "TAGS.IDX";
  private FileChannel indexFileChannel;
  private MappedByteBuffer indexMappedByteBuffer;
  private ByteBuf indexByteBuf;
  /**
   * bitmap file name format： tag_bitmap_000 tag_bitmap_001 ...
   */
  private static final String TAG_BITMAP_FILE = "TAGS.BITMAP";
  // 128M
  private static final long TAG_BITMAP_FILE_BLOCK_SIZE = 128 * 1024 * 1024;
  private FileChannel bitmapFileChannel;
  private MappedByteBuffer bitmapMappedByteBuffer;
  private ByteBuf bitmapByteBuf;
  private AtomicInteger bitmapWriteIdx = new AtomicInteger(0);

  public TagsIndexManager(Vertx vertx, Context context, IOffsetIndexStore offsetIndexStore) throws Exception {
    this.indexFilePath = CfgUtil.getString(ChronosConfig.CFG_META_STORAGE_PATH, context.config());
    this.indexMaxCapacity = CfgUtil.getInteger(ChronosConfig.CFG_META_TAGS_INDEX_CAPACITY, context.config());
    this.offsetIndexStore = offsetIndexStore;
    this.init();
  }

  /**
   * 当设备数量小于100万时，索引空间容量为400万桶，每个桶的大小为10， 总大小 100万 X 4 X 10 X 32 bytes = 1.19G 每当设备新增100万，将触发一次扩容，扩容计算方式： N百万 X 4 。。。。
   */
  private void init() throws Exception {
    File indexFile = new File(indexFilePath + File.separator + TAG_INDEX_FILE);
    if (!indexFile.exists()) {
      indexFileChannel = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      indexFileChannel.truncate(getExpectedIndexFileSize());
    } else {
      indexFileChannel = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }
    indexMappedByteBuffer = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexFileChannel.size());
    indexMappedByteBuffer.flip();
    indexByteBuf = Unpooled.wrappedBuffer(indexMappedByteBuffer);

    File bitmapFile = new File(indexFilePath + File.separator + TAG_BITMAP_FILE);
    if (!bitmapFile.exists()) {
      bitmapFileChannel = FileChannel.open(bitmapFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      bitmapFileChannel.truncate(TAG_BITMAP_FILE_BLOCK_SIZE);
      bitmapMappedByteBuffer = bitmapFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bitmapFileChannel.size());
      bitmapMappedByteBuffer.flip();
      bitmapByteBuf = Unpooled.wrappedBuffer(bitmapMappedByteBuffer);
      // 16 bytes bitmap file header.
      // magic value
      bitmapByteBuf.writeInt(0x870712);
      // version
      bitmapByteBuf.writeInt(0x01);
      // writer index
      bitmapByteBuf.writeInt(bitmapWriteIdx.get());
    } else {
      bitmapFileChannel = FileChannel.open(bitmapFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      bitmapMappedByteBuffer = bitmapFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bitmapFileChannel.size());
      bitmapMappedByteBuffer.flip();
      bitmapByteBuf = Unpooled.wrappedBuffer(bitmapMappedByteBuffer);
      bitmapWriteIdx.set(bitmapByteBuf.getInt(8));
    }
  }

  private long getExpectedIndexFileSize() {
    int size = offsetIndexStore.getSize();
    if (size < 1) {
      size = 1;
    }
    int num = Math.ceilDiv(size, indexMaxCapacity);
    return num * 100_0000L * 4 * 10 * 32;
  }

  public void addIndex(String tag, int metaDataId) throws Exception {
    if (tag.length() > 15) {
      throw new RuntimeException("tag's length can not be greater than 15");
    }
    checkIndexFileCapacity();
    checkBitmapFileCapacity();

    long pos = getPosition(tag);
    int status = indexByteBuf.getUnsignedByte((int) pos);
    // reserve 3 bytes
    if (status == 1) { // tag exits
      long offset = indexByteBuf.getLong((int) pos + 4);
      int length = indexByteBuf.getInt((int) pos + 12);


    } else { // tag not exits
      // append RoaringBitmap to byte buffer.
      int wrtIdx = bitmapWriteIdx.get();
      bitmapByteBuf.writerIndex(wrtIdx);
      // status 0: deleted 1: normal
      bitmapByteBuf.writeByte(1);
      bitmapByteBuf.writeInt((int) pos);
      int lengthIdx = bitmapByteBuf.writerIndex();
      bitmapByteBuf.writeInt(0);
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      roaringBitmap.add(metaDataId);
      int size = writeRoaringBitmap(roaringBitmap);
      bitmapByteBuf.setInt(lengthIdx, bitmapByteBuf.writerIndex() - lengthIdx - 4);
      bitmapWriteIdx.set(bitmapByteBuf.writerIndex());
      bitmapByteBuf.setInt(8, bitmapWriteIdx.get());
      indexByteBuf.setByte((int) pos, 1);
      indexByteBuf.setLong((int) (pos + 4), wrtIdx);
      indexByteBuf.setLong((int) (pos + 12), wrtIdx);
    }

    CodecUtil.setString(indexByteBuf, (int) pos, tag);
  }

  public void removeIndex(String tag, int metaDataId) throws Exception {

  }

  public IntIterator getMetaDataId(String... tag) {
    return null;
  }

  private void checkIndexFileCapacity() throws IOException {
    long size = getExpectedIndexFileSize();
    if (indexFileChannel.size() < size) {
      // TODO expand the index file size and reindex all tags.
      System.out.println("checkIndexFileCapacity");
    }
  }

  private void checkBitmapFileCapacity() {

  }

  private RoaringBitmap readRoaringBitmap(int offset, int length) throws Exception {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.deserialize(new ByteBufInputStream(bitmapByteBuf.slice(offset, length).readerIndex(0)));
    return roaringBitmap;
  }

  private int writeRoaringBitmap(RoaringBitmap bitmap) throws Exception {
    int size = bitmap.serializedSizeInBytes();
    bitmap.serialize(new ByteBufOutputStream(bitmapByteBuf));
    return size;
  }

  private long getPosition(String tag) {
    XXHash32 xxHash = new XXHash32(0);
    byte[] buf = tag.getBytes(StandardCharsets.UTF_8);
    xxHash.update(buf, 0, buf.length);
    return xxHash.getValue();
  }
}
