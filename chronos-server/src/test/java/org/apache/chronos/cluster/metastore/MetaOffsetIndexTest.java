package org.apache.chronos.cluster.metastore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class MetaOffsetIndexTest {

  @Test
  public void test() throws IOException {
    MetaOffsetIndex metaOffsetIndex = new MetaOffsetIndex(Path.of("/Users/xuyang/project/chronos/test.db"));

  }
}