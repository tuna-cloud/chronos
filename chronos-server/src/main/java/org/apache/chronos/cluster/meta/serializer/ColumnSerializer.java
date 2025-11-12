package org.apache.chronos.cluster.meta.serializer;

import com.apache.chronos.protocol.codec.CodecUtil;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
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
    CodecUtil.writeList(byteBuf, metaData.getTags());
    CodecUtil.writeMap(byteBuf, metaData.getAttrs());
  }

  @Override
  public Column deserialize(ByteBuf byteBuf) {
    Column column = Column.create();
    column.setId(CodecUtil.readVarInt(byteBuf));
    column.setCreatedAt(CodecUtil.readVarLong(byteBuf));
    column.setUpdatedAt(CodecUtil.readVarLong(byteBuf));
    column.setValueType(ValueType.fromValue(byteBuf.readUnsignedByte()));
    column.setCode(CodecUtil.readString(byteBuf));
    column.setTags(CodecUtil.readList(byteBuf));
    column.setAttrs(CodecUtil.readMap(byteBuf));
    return column;
  }
}
