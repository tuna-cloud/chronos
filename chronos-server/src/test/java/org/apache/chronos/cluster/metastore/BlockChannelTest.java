package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

class BlockChannelTest {

  @TempDir
  private Path path;

  @Test
  public void test() throws Exception {
    BlockChannel channel = new BlockChannel(path.toFile().getAbsolutePath() + File.separator + "test.bitmap");
    System.out.println(channel.getFileSize());
//    channel.expandChannelFile();
//    System.out.println(channel.getFileSize());
    Assertions.assertTrue(channel.getAvailableFileSize() > 0);
    Assertions.assertEquals(channel.getPageOffset(32 + 16 + 100), 32);
    Assertions.assertEquals(channel.getPageOffset(32 + 16 + 100 + 128 * 1024), 32 + 128 * 1024);
    Assertions.assertEquals(channel.getNextPageOffset(32 + 16 + 100), 32 + 128 * 1024);
    Assertions.assertEquals(channel.getUsedPageSize(32 + 16 + 100), 0);
    channel.addUsedPageSize(32 + 16 + 100, 300);
    Assertions.assertEquals(channel.getUsedPageSize(32 + 16 + 100), 300);
    Assertions.assertEquals(channel.getPageEntryNumber(32 + 16 + 100), 0);

    RoaringBitmap roaringBitmap = createOnePageMap(10000);
    int idx = channel.addRoaringBitmap(100, roaringBitmap);

    RoaringBitmap newMap = channel.getRoaringBitmap(idx);
    Assertions.assertEquals(newMap, roaringBitmap);


    RoaringBitmap roaringBitmap1 = createOnePageMap(10000);
    int idx1 = channel.addRoaringBitmap(100, roaringBitmap1);

    RoaringBitmap newMap1 = channel.getRoaringBitmap(idx1);
    Assertions.assertEquals(newMap1, roaringBitmap1);

    RoaringBitmap roaringBitmap2 = createOnePageMap(1000000);
//    testSeirial(roaringBitmap2);
    int idx2 = channel.addRoaringBitmap(100, roaringBitmap2);

    RoaringBitmap newMap2 = channel.getRoaringBitmap(idx2);
//    Assertions.assertEquals(newMap2, roaringBitmap2);
    for (int i = 0; i < 1000000; i++) {
      Assertions.assertTrue(roaringBitmap2.contains(i), "" + i);
    }

    for (int i = 0; i < 1000000; i++) {
      Assertions.assertTrue(newMap2.contains(i), "" + i);
    }
  }

  private RoaringBitmap createOnePageMap(int max) {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 0; i < max; i++) {
      roaringBitmap.add(i);
    }
    return roaringBitmap;
  }

  private void testSeirial(RoaringBitmap roaringBitmap) throws IOException {
    ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
    roaringBitmap.serialize(new ByteBufOutputStream(out));
    System.out.println("buf: " + ByteBufUtil.hexDump(out));
    RoaringBitmap map = new RoaringBitmap();
    map.deserialize(new ByteBufInputStream(out));
    for (int i = 0; i < 1000000; i++) {
      Assertions.assertTrue(map.contains(i), "" + i);
    }
    Assertions.assertEquals(map, roaringBitmap);
  }
}