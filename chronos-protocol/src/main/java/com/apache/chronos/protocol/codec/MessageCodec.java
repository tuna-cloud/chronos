package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.AbstractMessage;
import io.netty.buffer.ByteBuf;

public abstract class MessageCodec<T extends AbstractMessage> {

  public abstract T decode(ByteBuf byteBuf);

  public abstract void encode(ByteBuf out, T message);

  protected void decodeAbstractMessage(ByteBuf buf, T message) {
    message.setNodeId(CodecUtil.readString(buf, 1));
  }

  protected void encodeAbstractMessage(ByteBuf buf, T message) {
    CodecUtil.writeString(buf, 1, message.getNodeId());
  }
}
