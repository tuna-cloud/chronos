package com.apache.chronos.protocol.codec;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

  public static String getString(ByteBuf buf, int pos) {
    int length = getVarInt(buf, pos);
    if (length == 0) {
      return null;
    }
    return buf.getCharSequence(pos + getVarIntLength(length), length, StandardCharsets.UTF_8).toString();
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

  public static void setString(ByteBuf buf, int pos, String value) {
    if (StringUtil.isNullOrEmpty(value)) {
      setVarInt(buf, pos, 0);
    } else {
      byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
      setVarInt(buf, pos, valueBytes.length);
      buf.setBytes(pos + getVarIntLength(valueBytes.length), valueBytes);
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

  public static void setVarInt(ByteBuf buf, int pos, int value) {
    if (value < 64) {
      buf.setByte(pos, value);
    } else if (value < 16384) {
      buf.setByte(pos,(value >>> 8) | (0B01 << 6));
      buf.setByte(pos + 1,value & 0xFF);
    } else if (value < 4194304) {
      buf.setByte(pos,(value >>> 16) | (0B10 << 6));
      buf.setByte(pos + 1,(value >>> 8) & 0xFF);
      buf.setByte(pos + 2,value & 0xFF);
    } else if (value < 1073741824) {
      buf.setByte(pos,(value >>> 24) | (0B11 << 6));
      buf.setByte(pos + 1,(value >>> 16) & 0xFF);
      buf.setByte(pos + 2,(value >>> 8) & 0xFF);
      buf.setByte(pos + 3,value & 0xFF);
    } else {
      throw new RuntimeException("value is too big: " + value);
    }
  }

  public static int getVarIntLength(int value) {
    if (value < 64) {
      return 1;
    } else if (value < 16384) {
      return 2;
    } else if (value < 4194304) {
      return 3;
    } else if (value < 1073741824) {
      return 4;
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

  public static int getVarInt(ByteBuf buf, int pos) {
    int tmp = buf.getUnsignedByte(pos);
    int flag = (tmp >>> 6) & 0xFF;
    switch (flag) {
      case 0 -> {
        return tmp & 0x3F;
      }
      case 1 -> {
        return ((tmp & 0x3F) << 8) | buf.getUnsignedByte(pos + 1);
      }
      case 2 -> {
        return ((tmp & 0x3F) << 16) | buf.getUnsignedShort(pos + 1);
      }
      case 3 -> {
        return ((tmp & 0x3F) << 24) | buf.getUnsignedMedium(pos + 1);
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


  public static void writeMap(ByteBuf buf, Map<String, String> map) {
    if (map == null) {
      writeVarInt(buf, 0);
    } else {
      writeVarInt(buf, map.size());
      for (Entry<String, String> entry : map.entrySet()) {
        writeString(buf, entry.getKey());
        writeString(buf, entry.getValue());
      }
    }
  }

  public static Map<String, String> readMap(ByteBuf buf) {
    int size = readVarInt(buf);
    if (size < 1) {
      return null;
    }
    Map<String, String> map = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      map.put(readString(buf), readString(buf));
    }
    return map;
  }

  public static void writeList(ByteBuf buf, List<String> list) {
    if (list == null) {
      writeVarInt(buf, 0);
    } else {
      writeVarInt(buf, list.size());
      for (String s : list) {
        writeString(buf, s);
      }
    }
  }

  public static List<String> readList(ByteBuf buf) {
    int size = readVarInt(buf);
    if (size < 1) {
      return null;
    }
    List<String> list = Lists.newArrayListWithCapacity(size);
    for (int i = 0; i < size; i++) {
      list.add(readString(buf));
    }
    return list;
  }
}
