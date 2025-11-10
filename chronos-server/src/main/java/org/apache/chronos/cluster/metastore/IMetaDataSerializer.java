package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;

public interface IMetaDataSerializer {

  void serialize(ByteBuf byteBuf);

  void deserialize(ByteBuf byteBuf);
}
