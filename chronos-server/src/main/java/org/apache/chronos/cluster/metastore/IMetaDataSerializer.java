package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import org.apache.chronos.cluster.meta.IMetaData;

public interface IMetaDataSerializer<T extends IMetaData> {

  void serialize(ByteBuf byteBuf, T metaData);

  T deserialize(ByteBuf byteBuf);
}
