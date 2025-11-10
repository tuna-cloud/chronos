package org.apache.chronos.cluster.metastore;

import com.apache.chronos.protocol.codec.CodecUtil;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.chronos.cluster.meta.Column;
import org.apache.chronos.cluster.meta.ValueType;

public class ColumnSerializer implements IMetaDataSerializer<Column> {

  @Override
  public void serialize(ByteBuf byteBuf, Column metaData) {
    byteBuf.writeInt(metaData.getId());
    byteBuf.writeLong(metaData.getCreatedAt());
    byteBuf.writeLong(metaData.getUpdatedAt());
    byteBuf.writeByte(metaData.getValueType().getValue());
    CodecUtil.writeString(byteBuf, 1, metaData.getCode());
    Map<String, String> tags = metaData.getTags();
    if(tags != null && !tags.isEmpty()) {
      byteBuf.writeByte(tags.size());
      for (Entry<String, String> entry : tags.entrySet()) {
        CodecUtil.writeString(byteBuf, 1, entry.getKey());
        CodecUtil.writeString(byteBuf, 1, entry.getValue());
      }
    } else {
      byteBuf.writeByte(0);
    }
  }

  @Override
  public Column deserialize(ByteBuf byteBuf) {
    Column column = Column.create();
    column.setId(byteBuf.readInt());
    column.setCreatedAt(byteBuf.readLong());
    column.setUpdatedAt(byteBuf.readLong());
    column.setValueType(ValueType.fromValue(byteBuf.readUnsignedByte()));
    column.setCode(CodecUtil.readString(byteBuf, 1));
    int tags = byteBuf.readUnsignedByte();
    if (tags > 0) {
      column.setTags(Maps.newHashMapWithExpectedSize(tags));
      for (int i = 0; i < tags; i++) {
        String key = CodecUtil.readString(byteBuf, 1);
        String value = CodecUtil.readString(byteBuf, 1);
        column.getTags().put(key, value);
      }
    }
    return column;
  }
}
