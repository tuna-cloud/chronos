package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.AbstractMessage;
import com.apache.chronos.protocol.message.MessageType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * Protocol format field name
 * <p>
 * - | header | message type | message length | message body | crc check |
 * <p>
 * bytes length
 * <p>
 * - |   2    | 1 byte       | 2 byte         | n            | 1         |<p>
 * <p>
 * - |83 38   | 01           | 32 33          | ** ** ** **  | 32        |<p>
 */
public class MessageDecoder extends ByteToMessageDecoder {

  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
    // 校验最小长度
    if (byteBuf.readableBytes() < 6) {
      return;
    }
    // 校验包头
    byteBuf.markReaderIndex();
    if (byteBuf.readUnsignedShort() != 0x8338) {
      byteBuf.resetReaderIndex();
      byteBuf.skipBytes(1);
      return;
    }

    MessageType messageType = MessageType.parse(byteBuf.readUnsignedByte());
    int bodySize = byteBuf.readUnsignedShort();
    ByteBuf body = byteBuf.readSlice(bodySize);

    int cacBbc = CodecUtil.getBCC(body, body.readerIndex(), body.writerIndex());
    if (byteBuf.readUnsignedByte() != cacBbc) {
      // bbc check failed
      return;
    }

    MessageCodec codec = MessageCodecFactory.getCodec(messageType);
    AbstractMessage message = codec.decode(body);
    list.add(message);
  }
}
