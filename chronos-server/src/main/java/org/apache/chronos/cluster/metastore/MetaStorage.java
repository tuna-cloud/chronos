package org.apache.chronos.cluster.metastore;

import io.vertx.core.Context;
import java.util.Collection;
import java.util.List;
import org.apache.chronos.cluster.meta.IMetaData;

public class MetaStorage implements IMetaStorage {
  private final IMetaIndexManager indexManager;
  private final IMetaMemTable memTable;

  public MetaStorage(Context context) {
    indexManager = new MetaIndexManager();
    memTable = new MetaMemTable(new MetaBlockManager(), context);
  }

  @Override
  public IMetaData getById(int id) {
    Offset offset = indexManager.getById(id);
    if (offset == null) {
      return null;
    }
    return memTable.getById(id);
  }

  @Override
  public IMetaData getByCode(String code) {
    Offset offset = indexManager.getByCode(code);
    if (offset == null) {
      return null;
    }
    return memTable.getByCode(code);
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
