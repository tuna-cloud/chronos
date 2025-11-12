package org.apache.chronos.cluster.meta.serializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.chronos.cluster.meta.MultiplyColumn;
import org.apache.chronos.cluster.meta.ValueType;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MultiplyColumnSerializerTest {

  @Test
  public void test() {
    MultiplyColumn column = MultiplyColumn.create();
    column.setId(RandomUtils.secure().randomInt() >>> 2);
    column.setCreatedAt(RandomUtils.secure().randomLong() >>> 3);
    column.setUpdatedAt(RandomUtils.secure().randomLong() >>> 3);
    column.setCodes(Lists.newArrayList(new String(RandomUtils.secure().randomBytes(10))));
    column.setTypes(Lists.newArrayList(ValueType.BYTE));
    column.setTags(Lists.newArrayList(Long.toString(RandomUtils.secure().randomLong())));
    column.setAttrs(Maps.newHashMap());
    column.getAttrs().put(Long.toString(RandomUtils.secure().randomLong()), Long.toString(RandomUtils.secure().randomLong()));

    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
    MultiplyColumnSerializer.INSTANCE.serialize(buf, column);

    MultiplyColumn newColumn = MultiplyColumnSerializer.INSTANCE.deserialize(buf);

    Assertions.assertEquals(column.getId(), newColumn.getId());
    Assertions.assertEquals(column.getCreatedAt(), newColumn.getCreatedAt());
    Assertions.assertEquals(column.getUpdatedAt(), newColumn.getUpdatedAt());
    Assertions.assertEquals(column.getCodes(), newColumn.getCodes());
    Assertions.assertEquals(column.getTypes(), newColumn.getTypes());
    Assertions.assertEquals(column.getTags(), newColumn.getTags());
    Assertions.assertEquals(column.getAttrs(), newColumn.getAttrs());

    column.recycle();
    newColumn.recycle();
  }
}