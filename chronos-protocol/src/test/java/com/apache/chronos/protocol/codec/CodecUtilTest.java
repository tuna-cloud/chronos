package com.apache.chronos.protocol.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CodecUtilTest {

  @Test
  public void test() {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();

    String msg = "hellow";
    CodecUtil.writeString(byteBuf, msg);
    Assertions.assertEquals(CodecUtil.readString(byteBuf), msg);

    CodecUtil.writeVarInt(byteBuf, 32);
    CodecUtil.writeVarInt(byteBuf, 15384);
    CodecUtil.writeVarInt(byteBuf, 3194304);
    CodecUtil.writeVarInt(byteBuf, 1063741824);

    Assertions.assertEquals(CodecUtil.readVarInt(byteBuf), 32);
    Assertions.assertEquals(CodecUtil.readVarInt(byteBuf), 15384);
    Assertions.assertEquals(CodecUtil.readVarInt(byteBuf), 3194304);
    Assertions.assertEquals(CodecUtil.readVarInt(byteBuf), 1063741824);

    CodecUtil.writeVarLong(byteBuf, 24);
    CodecUtil.writeVarLong(byteBuf, 8191);
    CodecUtil.writeVarLong(byteBuf, 2097151);
    CodecUtil.writeVarLong(byteBuf, 536870911);
    CodecUtil.writeVarLong(byteBuf, 137438953471L);
    CodecUtil.writeVarLong(byteBuf, 35184372088831L);
    CodecUtil.writeVarLong(byteBuf, 9007199254740991L);
    CodecUtil.writeVarLong(byteBuf, 2305843009213693951L);

    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 24);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 8191);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 2097151);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 536870911);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 137438953471L);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 35184372088831L);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 9007199254740991L);
    Assertions.assertEquals(CodecUtil.readVarLong(byteBuf), 2305843009213693951L);
  }
}