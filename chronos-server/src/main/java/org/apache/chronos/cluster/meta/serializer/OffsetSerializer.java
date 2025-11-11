package org.apache.chronos.cluster.meta.serializer;

import io.netty.buffer.ByteBuf;
import org.apache.chronos.cluster.meta.Offset;

public class OffsetSerializer {

  public static OffsetSerializer INSTANCE = new OffsetSerializer();

  public void serialize(ByteBuf byteBuf, Offset offset) {
    byteBuf.writeByte(offset.getStatus());
    byteBuf.writeInt(offset.getBlockId());
    byteBuf.writeLong(offset.getOffset());
    byteBuf.writeInt(offset.getLength());
    byteBuf.writeLong(offset.getUpdated());
  }

  public Offset deserialize(ByteBuf byteBuf) {
    Offset offset = Offset.create();
    offset.setStatus(byteBuf.readByte());
    offset.setBlockId(byteBuf.readInt());
    offset.setOffset(byteBuf.readLong());
    offset.setLength(byteBuf.readInt());
    offset.setUpdated(byteBuf.readLong());
    return offset;
  }
}
