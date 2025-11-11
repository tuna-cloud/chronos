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

  public static String readString(ByteBuf buf) {
    int length = readVarInt(buf);
    if (length == 0) {
      return null;
    }
    return buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
  }

  public static void writeString(ByteBuf buf, String value) {
    if (StringUtil.isNullOrEmpty(value)) {
      writeVarInt(buf, 0);
    } else {
      byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
      writeVarInt(buf, valueBytes.length);
      buf.writeBytes(valueBytes);
    }
  }

  public static void writeVarInt(ByteBuf buf, int value) {
    if (value < 64) {
      buf.writeByte(value);
    } else if (value < 16384) {
      buf.writeByte((value >>> 8) | (0B01 << 6));
      buf.writeByte(value & 0xFF);
    } else if (value < 4194304) {
      buf.writeByte((value >>> 16) | (0B10 << 6));
      buf.writeByte((value >>> 8) & 0xFF);
      buf.writeByte(value & 0xFF);
    } else if (value < 1073741824) {
      buf.writeByte((value >>> 24) | (0B11 << 6));
      buf.writeByte((value >>> 16) & 0xFF);
      buf.writeByte((value >>> 8) & 0xFF);
      buf.writeByte(value & 0xFF);
    } else {
      throw new RuntimeException("value is too big: " + value);
    }
  }

  public static void writeVarLong(ByteBuf buf, long value) {
    if (value < 32) { // 2 ^ 5
      buf.writeByte((int) value);
    } else if (value < 8192) { // 2 ^ 13
      buf.writeByte((int) ((value >>> 8) | (0B001 << 5)));
      buf.writeByte((int) (value & 0xFF));
    } else if (value < 2097152) { // 2 ^ 21
      buf.writeByte((int) ((value >>> 16) | (0B010 << 5)));
      buf.writeByte((int) ((value >>> 8) & 0xFF));
      buf.writeByte((int) (value & 0xFF));
    } else if (value < 536870912) { // 2 ^ 29
      buf.writeByte((int) ((value >>> 24) | (0B011 << 5)));
      buf.writeByte((int) ((value >>> 16) & 0xFF));
      buf.writeByte((int) ((value >>> 8) & 0xFF));
      buf.writeByte((int) (value & 0xFF));
    } else if (value < 137438953472L) { // 2 ^ 37
      buf.writeByte((int) ((value >>> 32) | (0B100 << 5)));
      buf.writeByte((int) ((value >>> 24) & 0xFF));
      buf.writeByte((int) ((value >>> 16) & 0xFF));
      buf.writeByte((int) ((value >>> 8) & 0xFF));
      buf.writeByte((int) (value & 0xFF));
    } else if (value < 35184372088832L) { // 2 ^ 45
      buf.writeByte((int) ((value >>> 40) | (0B101 << 5)));
      buf.writeByte((int) ((value >>> 32) & 0xFF));
      buf.writeByte((int) ((value >>> 24) & 0xFF));
      buf.writeByte((int) ((value >>> 16) & 0xFF));
      buf.writeByte((int) ((value >>> 8) & 0xFF));
      buf.writeByte((int) (value & 0xFF));
    } else if (value < 9007199254740992L) { // 2 ^ 53
      buf.writeByte((int) ((value >>> 48) | (0B110 << 5)));
      buf.writeByte((int) ((value >>> 40) & 0xFF));
      buf.writeByte((int) ((value >>> 32) & 0xFF));
      buf.writeByte((int) ((value >>> 24) & 0xFF));
      buf.writeByte((int) ((value >>> 16) & 0xFF));
      buf.writeByte((int) ((value >>> 8) & 0xFF));
      buf.writeByte((int) (value & 0xFF));
    } else if (value < 2305843009213693952L) { // 2 ^ 61
      buf.writeByte((int) ((value >>> 56) | (0B111 << 5)));
      buf.writeByte((int) ((value >>> 48) & 0xFF));
      buf.writeByte((int) ((value >>> 40) & 0xFF));
      buf.writeByte((int) ((value >>> 32) & 0xFF));
      buf.writeByte((int) ((value >>> 24) & 0xFF));
      buf.writeByte((int) ((value >>> 16) & 0xFF));
      buf.writeByte((int) ((value >>> 8) & 0xFF));
      buf.writeByte((int) (value & 0xFF));
    } else {
      throw new RuntimeException("value is too big: " + value);
    }
  }

  public static int readVarInt(ByteBuf buf) {
    int tmp = buf.readUnsignedByte();
    int flag = (tmp >>> 6) & 0xFF;
    switch (flag) {
      case 0 -> {
        return tmp & 0x3F;
      }
      case 1 -> {
        return ((tmp & 0x3F) << 8) | buf.readUnsignedByte();
      }
      case 2 -> {
        return ((tmp & 0x3F) << 16) | buf.readUnsignedShort();
      }
      case 3 -> {
        return ((tmp & 0x3F) << 24) | buf.readUnsignedMedium();
      }
      default -> {
        throw new RuntimeException("var int flag exception: " + flag);
      }
    }
  }

  public static long readVarLong(ByteBuf buf) {
    long tmp = buf.readUnsignedByte();
    int flag = (int) ((tmp >>> 5) & 0xFF);
    switch (flag) {
      case 0 -> {
        return tmp & 0x1F;
      }
      case 1 -> {
        return ((tmp & 0x1F) << 8) | buf.readUnsignedByte();
      }
      case 2 -> {
        return ((tmp & 0x1F) << 16) | buf.readUnsignedShort();
      }
      case 3 -> {
        return ((tmp & 0x1F) << 24) | buf.readUnsignedMedium();
      }
      case 4 -> {
        return ((tmp & 0x1F) << 32) | buf.readUnsignedInt();
      }
      case 5 -> {
        return ((tmp & 0x1F) << 40) | (((long) buf.readUnsignedByte()) << 32) | buf.readUnsignedInt();
      }
      case 6 -> {
        return ((tmp & 0x1F) << 48) | (((long) buf.readUnsignedShort()) << 32) | buf.readUnsignedInt();
      }
      case 7 -> {
        return ((tmp & 0x1F) << 56) | (((long) buf.readUnsignedMedium()) << 32) | buf.readUnsignedInt();
      }
      default -> {
        throw new RuntimeException("var int flag exception: " + flag);
      }
    }
  }
}
