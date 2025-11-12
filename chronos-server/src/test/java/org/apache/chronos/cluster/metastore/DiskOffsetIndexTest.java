package org.apache.chronos.cluster.metastore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.chronos.cluster.meta.Offset;
import org.apache.chronos.cluster.meta.OffsetTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskOffsetIndexTest {

  @TempDir
  private Path path;

  @Test
  public void test() throws IOException {
    DiskOffsetIndex diskOffsetIndex = new DiskOffsetIndex(new File(path.toFile().getAbsolutePath() + File.separator + "/test.db"));

    Offset offset1 = OffsetTest.create();
    Offset offset2 = OffsetTest.create();
    Offset offset3 = OffsetTest.create();
    diskOffsetIndex.upsertOffset(1, offset1);
    diskOffsetIndex.upsertOffset(100, offset2);
    diskOffsetIndex.upsertOffset(101, offset3);

    Assertions.assertEquals(diskOffsetIndex.getMetaDataVersion(), 3);
    Assertions.assertEquals(diskOffsetIndex.getSize(), 3);
    Assertions.assertEquals(diskOffsetIndex.getMaxMetaDataId(), 101);

    Offset offset1New = diskOffsetIndex.getOffset(1);
    Offset offset2New = diskOffsetIndex.getOffset(100);
    Offset offset3New = diskOffsetIndex.getOffset(101);

    OffsetTest.assertEquals(offset1, offset1New);
    OffsetTest.assertEquals(offset2, offset2New);
    OffsetTest.assertEquals(offset3, offset3New);

    diskOffsetIndex.removeOffset(101);
    Assertions.assertEquals(diskOffsetIndex.getSize(), 2);
    Assertions.assertEquals(diskOffsetIndex.getMetaDataVersion(), 4);
    Assertions.assertEquals(diskOffsetIndex.getMaxMetaDataId(), 100);
    diskOffsetIndex.removeOffset(100);
    Assertions.assertEquals(diskOffsetIndex.getMaxMetaDataId(), 1);

    offset3New = diskOffsetIndex.getOffset(101);
    Assertions.assertNull(offset3New);
  }
}