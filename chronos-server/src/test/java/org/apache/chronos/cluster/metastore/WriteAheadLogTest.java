package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {

  @TempDir
  private Path path;

  @Test
  public void test() throws IOException {
    String file = path.toFile().getAbsolutePath() + File.separator + "/test.db";
    WriteAheadLog writeAheadLog = new WriteAheadLog(file, 4096);
    for (int i = 0; i < 100; i++) {
      writeAheadLog.append(RandomUtils.secure().randomInt());
    }
    writeAheadLog.commit();

    ByteBuf byteBuf = writeAheadLog.readFrom(0, 4096);
    Assertions.assertEquals(byteBuf.readableBytes(), 100 * 4);
    ReferenceCountUtil.safeRelease(byteBuf);

    writeAheadLog.truncateTo(300);
    byteBuf = writeAheadLog.readFrom(0, 4096);
    Assertions.assertEquals(byteBuf.readableBytes(), 100);
  }
}