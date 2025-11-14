package org.apache.chronos.cluster.metastore;

import com.google.common.collect.Maps;
import java.util.Map;

public class TagsIndexManager {

  private String indexFilePath;
  private String roaringBitmapFilePath;
  private Map<String, TagsIndexEntry> tagsIndexMap = Maps.newConcurrentMap();


  public void init() {
    // 从磁盘读取索引，构建tagsIndexMap
    // 索引文件有几个特点：仅追加和修改，不能物理删除释放空间
  }
}
