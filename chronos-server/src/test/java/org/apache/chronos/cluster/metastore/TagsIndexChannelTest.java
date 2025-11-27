package org.apache.chronos.cluster.metastore;

import static org.junit.jupiter.api.Assertions.*;

import io.vertx.core.Future;
import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.chronos.cluster.meta.IMetaData;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TagsIndexChannelTest {
  @TempDir
  private Path path;
  @Test
  public void test() throws Exception {
    TagsIndexChannel channel = new TagsIndexChannel(path.toFile().getAbsolutePath() , 1000000, new IStorageEngine() {
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
        return 0;
      }

      @Override
      public int getSize() {
        return 500;
      }
    });

    channel.addIndex("test", 100, 100);
    Pair<Integer, Integer> pair = channel.getBlockOffset("test");
    Assertions.assertEquals(pair.getLeft(), 100);
    Assertions.assertEquals(pair.getRight(), 100);
    channel.removeIndex("test");
    Assertions.assertNull(channel.getBlockOffset("test"));
    channel.addIndex("test_754256", 200, 300);
    channel.addIndex("test_999976", 400, 500);
    Pair<Integer, Integer> pair1 = channel.getBlockOffset("test_754256");
    Assertions.assertEquals(pair1.getLeft(), 200);
    Assertions.assertEquals(pair1.getRight(), 300);
    Pair<Integer, Integer> pair2 = channel.getBlockOffset("test_999976");
    Assertions.assertEquals(pair2.getLeft(), 400);
    Assertions.assertEquals(pair2.getRight(), 500);
    channel.removeIndex("test_754256");
    Assertions.assertNull(channel.getBlockOffset("test_754256"));
    pair2 = channel.getBlockOffset("test_999976");
    Assertions.assertEquals(pair2.getLeft(), 400);
    Assertions.assertEquals(pair2.getRight(), 500);

  }
}