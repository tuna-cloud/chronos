package org.apache.chronos.cluster.meta.serializer;

import com.apache.chronos.protocol.codec.CodecUtil;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.chronos.cluster.meta.Column;
import org.apache.chronos.cluster.meta.ValueType;

public class ColumnSerializer implements IMetaDataSerializer<Column> {

  public static ColumnSerializer INSTANCE = new ColumnSerializer();

  @Override
  public void serialize(ByteBuf byteBuf, Column metaData) {
    CodecUtil.writeVarInt(byteBuf, metaData.getId());
    CodecUtil.writeVarLong(byteBuf, metaData.getCreatedAt());
    CodecUtil.writeVarLong(byteBuf, metaData.getUpdatedAt());
    byteBuf.writeByte(metaData.getValueType().getValue());
    CodecUtil.writeString(byteBuf, metaData.getCode());
    List<String> tags = metaData.getTags();
    if (tags != null && !tags.isEmpty()) {
      byteBuf.writeByte(tags.size());
      for (String tag : tags) {
        CodecUtil.writeString(byteBuf, tag);
      }
    } else {
      byteBuf.writeByte(0);
    }
  }

  @Override
  public Column deserialize(ByteBuf byteBuf) {
    Column column = Column.create();
    column.setId(CodecUtil.readVarInt(byteBuf));
    column.setCreatedAt(CodecUtil.readVarLong(byteBuf));
    column.setUpdatedAt(CodecUtil.readVarLong(byteBuf));
    column.setValueType(ValueType.fromValue(byteBuf.readUnsignedByte()));
    column.setCode(CodecUtil.readString(byteBuf));
    int tags = byteBuf.readUnsignedByte();
    if (tags > 0) {
      column.setTags(Lists.newArrayListWithCapacity(tags));
      for (int i = 0; i < tags; i++) {
        column.getTags().add(CodecUtil.readString(byteBuf));
      }
    }
    return column;
  }
}
