package org.apache.chronos.cluster.metastore;

public class TagsIndexManager {

  private String indexFilePath;

  /**
   * 当设备数量小于100万时，索引空间容量为400万桶，每个桶的大小为10， 总大小 100万 X 4 X 10 X 32 bytes = 1.19G
   * 每当设备新增100万，将触发一次扩容，扩容计算方式： N百万 X 4 。。。。
   */

  public void init() {
    // 从磁盘读取索引，构建tagsIndexMap
    // 索引文件有几个特点：仅追加和修改，不能物理删除释放空间
  }
}
