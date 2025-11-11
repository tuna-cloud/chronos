package org.apache.chronos.cluster.metastore;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.chronos.cluster.meta.IMetaData;

public class StorageEngine implements IStorageEngine {

  private final Context context;
  private final Vertx vertx;
  private final ConcurrentNavigableMap<Integer, IMetaData> metadataMap = new ConcurrentSkipListMap<>();

  public StorageEngine(Vertx vertx, Context context) {
    this.context = context;
    this.vertx = vertx;
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
  public long getVersion() {
    return 0;
  }
}
