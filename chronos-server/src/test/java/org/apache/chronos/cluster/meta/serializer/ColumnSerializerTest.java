package org.apache.chronos.cluster.meta.serializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.chronos.cluster.meta.Column;
import org.apache.chronos.cluster.meta.ValueType;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ColumnSerializerTest {

  @Test
  public void test() {
    Column column = Column.create();
    column.setId(RandomUtils.secure().randomInt() >>> 2);
    column.setCreatedAt(RandomUtils.secure().randomLong() >>> 3);
    column.setUpdatedAt(RandomUtils.secure().randomLong() >>> 3);
    column.setCode(new String(RandomUtils.secure().randomBytes(10)));
    column.setValueType(ValueType.BYTE);
    column.setTags(Lists.newArrayList(Long.toString(RandomUtils.secure().randomLong())));
    column.setAttrs(Maps.newHashMap());
    column.getAttrs().put(Long.toString(RandomUtils.secure().randomLong()), Long.toString(RandomUtils.secure().randomLong()));

    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
    ColumnSerializer.INSTANCE.serialize(buf, column);

    Column newColumn = ColumnSerializer.INSTANCE.deserialize(buf);

    Assertions.assertEquals(column.getId(), newColumn.getId());
    Assertions.assertEquals(column.getCreatedAt(), newColumn.getCreatedAt());
    Assertions.assertEquals(column.getUpdatedAt(), newColumn.getUpdatedAt());
    Assertions.assertEquals(column.getCode(), newColumn.getCode());
    Assertions.assertEquals(column.getValueType(), newColumn.getValueType());
    Assertions.assertEquals(column.getTags(), newColumn.getTags());
    Assertions.assertEquals(column.getAttrs(), newColumn.getAttrs());

    column.recycle();
    newColumn.recycle();
  }

}