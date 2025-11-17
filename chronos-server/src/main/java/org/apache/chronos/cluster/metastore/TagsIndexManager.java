package org.apache.chronos.cluster.metastore;

import java.nio.channels.FileChannel;
import java.util.List;
import org.roaringbitmap.IntIterator;

public class TagsIndexManager {

  private String indexFilePath;
  /**
   * index file name format： tag_index_000 tag_index_001 ...
   */
  private List<FileChannel> indexFileChannels;
  /**
   * bitmap file name format： tag_bitmap_000 tag_bitmap_001 ...
   */
  private List<FileChannel> bitmapFileChannels;

  /**
   * 当设备数量小于100万时，索引空间容量为400万桶，每个桶的大小为10， 总大小 100万 X 4 X 10 X 32 bytes = 1.19G 每当设备新增100万，将触发一次扩容，扩容计算方式： N百万 X 4 。。。。
   */
  private void init() {
    // 从磁盘读取索引，构建tagsIndexMap
    // 索引文件有几个特点：仅追加和修改，不能物理删除释放空间
  }

  public void addIndex(TagsIndexEntry entry, int metaDataId) {

  }

  private TagsIndexEntry getIndexEntry(String tag) {
    return null;
  }

  private void addIndexEntry(TagsIndexEntry entry, int metaDataId) {

  }

  public IntIterator getMetaDataId(String... tag) {
    return null;
  }
}
