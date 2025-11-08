package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.Ping;
import io.netty.buffer.ByteBuf;

public class PingMessageCodec extends MessageCodec<Ping> {

  @Override
  public Ping decode(ByteBuf byteBuf) {
    Ping ping = Ping.create();
    super.decodeAbstractMessage(byteBuf, ping);
    return ping;
  }

  @Override
  public void encode(ByteBuf out, Ping message) {
    super.encodeAbstractMessage(out, message);
    message.recycle();
  }
}
