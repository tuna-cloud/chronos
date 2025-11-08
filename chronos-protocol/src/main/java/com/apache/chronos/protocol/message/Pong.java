package com.apache.chronos.protocol.message;

import io.netty.util.Recycler;

public class Pong extends AbstractMessage {

  private final Recycler.Handle<Pong> handle;

  public Pong(Recycler.Handle<Pong> handle) {
    this.handle = handle;
  }

  @Override
  public void recycle() {
    super.recycle();
    handle.recycle(this);
  }

  private static final Recycler<Pong> RECYCLER = new Recycler<Pong>() {
    @Override
    protected Pong newObject(Handle<Pong> handle) {
      return new Pong(handle);
    }
  };

  public static Pong create() {
    return RECYCLER.get();
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.PONG;
  }
}
