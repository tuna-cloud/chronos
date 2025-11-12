package org.apache.chronos.cluster.meta;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;

public class OffsetTest {

  public static Offset create() {
    Offset offset = Offset.create();
    offset.setOffset(RandomUtils.secure().randomLong() >>> 3);
    offset.setStatus((byte) Offset.STATUS_NORMAL);
    offset.setBlockId(RandomUtils.secure().randomInt() >> 2);
    offset.setLength(RandomUtils.secure().randomInt() >> 2);
    offset.setUpdated(System.currentTimeMillis());
    return offset;
  }

  public static void assertEquals(Offset offset1, Offset offset2) {
    Assertions.assertEquals(offset1.getOffset(), offset2.getOffset());
    Assertions.assertEquals(offset1.getStatus(), offset2.getStatus());
    Assertions.assertEquals(offset1.getUpdated(), offset2.getUpdated());
    Assertions.assertEquals(offset1.getLength(), offset2.getLength());
    Assertions.assertEquals(offset1.getBlockId(), offset2.getBlockId());
  }
}