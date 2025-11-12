package org.apache.chronos.cluster.metastore;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.chronos.cluster.meta.Offset;

public interface IOffsetIndexStore {

  void upsertOffset(int metaDataId, Offset offset) throws IOException;

  Offset getOffset(int metaDataId) throws ExecutionException;

  void removeOffset(int metaDataId) throws IOException;

  int getMetaDataVersion();

  int getSize();
}
