package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.AbstractMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

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
public class MessageEncoder extends MessageToByteEncoder<AbstractMessage> {

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext, AbstractMessage abstractMessage, ByteBuf byteBuf) throws Exception {

    byteBuf.writeShort(0x8338);
    byteBuf.writeByte(abstractMessage.getMessageType().getValue());
    int dataLengthFieldIdx = byteBuf.writerIndex();
    byteBuf.writeShort(0);
    int bccWrtIdx = byteBuf.writerIndex();
    MessageCodec codec = MessageCodecFactory.getCodec(abstractMessage.getMessageType());
    codec.encode(byteBuf, abstractMessage);
    /**
     * dynamic calculate body length
     */
    byteBuf.setShort(dataLengthFieldIdx, byteBuf.writerIndex() - dataLengthFieldIdx - 2);
    /**
     * update crc check field
     */
    byteBuf.writeByte(CodecUtil.getBCC(byteBuf, bccWrtIdx, byteBuf.writerIndex()));
  }
}
