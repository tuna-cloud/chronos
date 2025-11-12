package org.apache.chronos.cluster.metastore;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.chronos.cluster.meta.IMetaData;
import org.apache.chronos.common.CfgUtil;
import org.apache.chronos.common.ChronosConfig;

public class StorageEngine implements IStorageEngine {

  private final Context context;
  private final Vertx vertx;
  private final IOffsetIndexStore offsetIndexStore;

  public StorageEngine(Vertx vertx, Context context) throws IOException {
    this.context = context;
    this.vertx = vertx;
    this.offsetIndexStore = new MemoryOffsetIndexStoreWrapper(new DiskOffsetIndex(new File(CfgUtil.getString(ChronosConfig.CFG_META_STORAGE_PATH, context.config()))));
  }

  @Override
  public Future<Void> init() {
    // 1. load index
    // 2. load meta data
    // 3.
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

  @Override
  public List<IMetaData> listByTags(int pageNo, int offset, String... tags) {
    return null;
  }

  @Override
  public int countByTags(String... tags) {
    return 0;
  }

  @Override
  public void save(IMetaData metaData) {

  }

  @Override
  public void save(Collection<IMetaData> metaData) {

  }

  @Override
  public void update(IMetaData metaData) {

  }

  @Override
  public int getVersion() {
    return offsetIndexStore.getMetaDataVersion();
  }

  @Override
  public int getSize() {
    return 0;
  }
}
