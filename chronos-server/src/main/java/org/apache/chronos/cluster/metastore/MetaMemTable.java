package org.apache.chronos.cluster.metastore;

import io.vertx.core.Context;
import io.vertx.core.Future;
import org.apache.chronos.cluster.meta.IMetaData;

public class MetaMemTable implements IMetaMemTable {
  private final Context context;
  private final IMetaBlockManager blockManager;

  public MetaMemTable(IMetaBlockManager blockManager, Context context) {
    this.blockManager = blockManager;
    this.context = context;
  }

  @Override
  public Future<Void> init() {
    return null;
  }

  @Override
  public IMetaData getById(int id) {
    return null;
  }

  @Override
  public IMetaData getByCode(String code) {
    return null;
  }
}
