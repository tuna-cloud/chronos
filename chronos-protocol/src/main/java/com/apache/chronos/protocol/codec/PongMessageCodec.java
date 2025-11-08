package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.Pong;
import io.netty.buffer.ByteBuf;

public class PongMessageCodec extends MessageCodec<Pong> {

  @Override
  public Pong decode(ByteBuf byteBuf) {
    Pong pong = Pong.create();
    super.decodeAbstractMessage(byteBuf, pong);
    return pong;
  }

  @Override
  public void encode(ByteBuf out, Pong message) {
    super.encodeAbstractMessage(out, message);
    message.recycle();
  }
}
