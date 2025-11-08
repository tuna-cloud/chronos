package com.apache.chronos.protocol.message;

public abstract class AbstractMessage {

  private String nodeId;

  public abstract MessageType getMessageType();


  public void recycle() {
    nodeId = null;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }
}
