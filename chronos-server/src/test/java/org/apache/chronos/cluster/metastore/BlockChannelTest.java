package org.apache.chronos.cluster.metastore;

import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BlockChannelTest {

  @TempDir
  private Path path;

  @Test
  public void test() throws Exception {
    BlockChannel channel = new BlockChannel(path.toFile().getAbsolutePath() + File.separator + "test.bitmap");
    System.out.println(channel.getFileSize());
    channel.expandChannelFile();
    System.out.println(channel.getFileSize());
  }
}