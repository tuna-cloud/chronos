package com.apache.chronos.protocol.codec;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;
import java.nio.charset.StandardCharsets;

public final class CodecUtil {

  /**
   * @param buf
   * @param startIndex
   * @param endIndex
   * @return
   */
  public static int getBCC(ByteBuf buf, int startIndex, int endIndex) {
    int value = 0;
    for (int i = startIndex; i < endIndex; i++) {
      if (i == startIndex) {
        value = buf.getUnsignedByte(i);
      } else {
        value = value ^ buf.getUnsignedByte(i);
      }
    }
    return value;
  }

  public static String readString(ByteBuf buf, int fieldLength) {
    int length = 0;
    if (fieldLength == 1) {
      length = buf.readUnsignedByte();
    } else if (fieldLength == 2) {
      length = buf.readUnsignedShort();
    } else {
      throw new RuntimeException("Un support field length: " + fieldLength);
    }
    if (length == 0) {
      return null;
    }
    return buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
  }

  public static void writeString(ByteBuf buf, int fieldLength, String value) {
    if (fieldLength == 1) {
      if (StringUtil.isNullOrEmpty(value)) {
        buf.writeByte(0);
      } else {
        buf.writeByte(value.length());
        buf.writeCharSequence(value, StandardCharsets.UTF_8);
      }
    } else if (fieldLength == 2) {
      if (StringUtil.isNullOrEmpty(value)) {
        buf.writeShort(0);
      } else {
        buf.writeShort(value.length());
        buf.writeCharSequence(value, StandardCharsets.UTF_8);
      }
    } else {
      throw new RuntimeException("Un support field length: " + fieldLength);
    }
  }
}
