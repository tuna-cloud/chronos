package com.apache.chronos.protocol.message;


public enum MessageType {
  PING(0x01, "Ping message"),
  PONG(0x02, "Pong message");

  private final int value;
  private final String description;

  MessageType(int value, String description) {
    this.value = value;
    this.description = description;
  }

  public static MessageType parse(int value) {
    for (MessageType messageType : MessageType.values()) {
      if (messageType.getValue() == value) {
        return messageType;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }

  public String getDescription() {
    return description;
  }
}
