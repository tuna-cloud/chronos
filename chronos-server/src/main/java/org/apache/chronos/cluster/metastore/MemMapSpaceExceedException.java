package org.apache.chronos.cluster.metastore;

public class MemMapSpaceExceedException extends RuntimeException {

  public MemMapSpaceExceedException() {
    super();
  }

  public MemMapSpaceExceedException(String message) {
    super(message);
  }
}
