package org.apache.chronos.cluster.metastore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.chronos.cluster.meta.Offset;

public class MemoryOffsetIndexStoreWrapper implements IOffsetIndexStore {

  private final DiskOffsetIndex diskOffsetIndex;
  private final LoadingCache<Integer, Offset> cache;

  public MemoryOffsetIndexStoreWrapper(DiskOffsetIndex diskOffsetIndex) {
    this.diskOffsetIndex = diskOffsetIndex;
    cache = CacheBuilder.newBuilder().expireAfterWrite(24, TimeUnit.HOURS).maximumSize(100_0000L).build(new CacheLoader<Integer, Offset>() {
      @Override
      public Offset load(Integer key) throws Exception {
        return diskOffsetIndex.getOffset(key);
      }
    });
  }

  @Override
  public void upsertOffset(int metaDataId, Offset offset) throws IOException {
    cache.invalidate(metaDataId);
    diskOffsetIndex.upsertOffset(metaDataId, offset);
  }

  @Override
  public Offset getOffset(int metaDataId) throws ExecutionException {
    return cache.get(metaDataId);
  }

  @Override
  public void removeOffset(int metaDataId) throws IOException {
    cache.invalidate(metaDataId);
    diskOffsetIndex.removeOffset(metaDataId);
  }

  @Override
  public int getMetaDataVersion() {
    return diskOffsetIndex.getMetaDataVersion();
  }

  @Override
  public int getSize() {
    return diskOffsetIndex.getSize();
  }
}
