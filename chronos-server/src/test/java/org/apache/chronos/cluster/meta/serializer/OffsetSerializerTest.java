package org.apache.chronos.cluster.meta.serializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.chronos.cluster.meta.Offset;
import org.apache.chronos.cluster.meta.OffsetTest;
import org.junit.jupiter.api.Test;

class OffsetSerializerTest {

  @Test
  public void test() {
    Offset offset = OffsetTest.create();
    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();

    OffsetSerializer.INSTANCE.serialize(buf, offset);

    Offset newOffset = OffsetSerializer.INSTANCE.deserialize(buf);

    OffsetTest.assertEquals(offset, newOffset);
  }
}