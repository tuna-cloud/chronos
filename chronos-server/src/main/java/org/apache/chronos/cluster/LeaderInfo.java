package org.apache.chronos.cluster;

public class LeaderInfo {
  private String nodeId;
  private String host;
  private Integer port;

  public LeaderInfo() {
  }

  public LeaderInfo(String nodeId, String host, Integer port) {
    this.nodeId = nodeId;
    this.host = host;
    this.port = port;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }
}
