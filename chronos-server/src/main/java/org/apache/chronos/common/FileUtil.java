package org.apache.chronos.common;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileUtil {

  private static final Logger log = LogManager.getLogger(FileUtil.class);

  public static void clean(MappedByteBuffer buffer) {
    if (buffer == null) {
      return;
    }
    try {
      Method cleaner = buffer.getClass().getMethod("cleaner");
      cleaner.setAccessible(true);
      Object clean = cleaner.invoke(buffer);
      if (clean != null) {
        clean.getClass().getMethod("clean").invoke(clean);
      }
    } catch (Exception e) {
      log.error("Meta Index clean failed", e);
    }
  }
}
