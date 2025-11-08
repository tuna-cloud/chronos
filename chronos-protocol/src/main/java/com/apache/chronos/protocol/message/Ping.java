package com.apache.chronos.protocol.message;

import io.netty.util.Recycler;

public class Ping extends AbstractMessage {

  private final Recycler.Handle<Ping> handle;

  public Ping(Recycler.Handle<Ping> handle) {
    this.handle = handle;
  }

  @Override
  public void recycle() {
    super.recycle();
    handle.recycle(this);
  }

  private static final Recycler<Ping> RECYCLER = new Recycler<Ping>() {
    @Override
    protected Ping newObject(Handle<Ping> handle) {
      return new Ping(handle);
    }
  };

  public static Ping create() {
    return RECYCLER.get();
  }


  @Override
  public MessageType getMessageType() {
    return MessageType.PING;
  }
}
