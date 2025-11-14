package org.apache.chronos.cluster.metastore;

import com.google.common.collect.Maps;
import java.util.Map;

public class TagsIndexManager {

  private String indexFilePath;
  private String roaringBitmapFilePath;
  private Map<String, TagsIndexManager> tagsIndexMap = Maps.newConcurrentMap();


  public void init() {
    // 从磁盘读取索引，构建tagsIndexMap
  }
}
