package org.apache.chronos.cluster.meta.serializer;

import com.apache.chronos.protocol.codec.CodecUtil;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.chronos.cluster.meta.MultiplyColumn;
import org.apache.chronos.cluster.meta.ValueType;

public class MultiplyColumnSerializer implements IMetaDataSerializer<MultiplyColumn> {

  public static MultiplyColumnSerializer INSTANCE = new MultiplyColumnSerializer();

  @Override
  public void serialize(ByteBuf byteBuf, MultiplyColumn metaData) {
    byteBuf.writeInt(metaData.getId());
    byteBuf.writeLong(metaData.getCreatedAt());
    byteBuf.writeLong(metaData.getUpdatedAt());
    CodecUtil.writeList(byteBuf, metaData.getTags());
    CodecUtil.writeList(byteBuf, metaData.getCodes());
    List<ValueType> types = metaData.getTypes();
    if (types != null && !types.isEmpty()) {
      byteBuf.writeByte(types.size());
      for (ValueType type : types) {
        byteBuf.writeByte(type.getValue());
      }
    } else {
      byteBuf.writeByte(0);
    }
    CodecUtil.writeMap(byteBuf, metaData.getAttrs());
  }

  @Override
  public MultiplyColumn deserialize(ByteBuf byteBuf) {
    MultiplyColumn column = MultiplyColumn.create();
    column.setId(byteBuf.readInt());
    column.setCreatedAt(byteBuf.readLong());
    column.setUpdatedAt(byteBuf.readLong());
    column.setTags(CodecUtil.readList(byteBuf));
    column.setCodes(CodecUtil.readList(byteBuf));
    int length = byteBuf.readUnsignedByte();
    if (length > 0) {
      column.setTypes(Lists.newArrayListWithCapacity(length));
      for (int i = 0; i < length; i++) {
        column.getTypes().add(ValueType.fromValue(byteBuf.readUnsignedByte()));
      }
    }
    column.setAttrs(CodecUtil.readMap(byteBuf));
    return column;
  }
}
