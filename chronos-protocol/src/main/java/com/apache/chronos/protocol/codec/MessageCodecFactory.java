package com.apache.chronos.protocol.codec;

import com.apache.chronos.protocol.message.MessageType;
import java.util.HashMap;
import java.util.Map;

public class MessageCodecFactory {

  private static Map<MessageType, MessageCodec> map = new HashMap<>();

  static {
    map.put(MessageType.PING, new PingMessageCodec());
    map.put(MessageType.PONG, new PongMessageCodec());
  }

  public static MessageCodec getCodec(MessageType messageType) {
    return map.get(messageType);
  }
}
