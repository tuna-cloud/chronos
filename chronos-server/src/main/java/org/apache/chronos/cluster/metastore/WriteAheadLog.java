package org.apache.chronos.cluster.metastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog {
  /**
   * Just record all meta data id. All MetaData Follower update its meta data by metaDataId
   */
  private final FileChannel fileChannel;
  private final ByteBuffer writeBuffer;
  private final String filePath;
  private final int bufferSize;
  private long committedPosition = 0;

  public WriteAheadLog(String filePath, int bufferSize) throws IOException {
    this.filePath = filePath;
    this.bufferSize = bufferSize;

    this.fileChannel = FileChannel.open(
        Paths.get(filePath),
        StandardOpenOption.CREATE,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND  // 关键：追加模式
    );

    this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);
  }

  /**
   * 写入WAL记录
   * @param metaDataId 记录数据
   * @return 写入的文件位置
   */
  public long append(int metaDataId) throws IOException {
    synchronized (this) {
      long position = fileChannel.position();
      writeBuffer.putInt(metaDataId);
      return position;
    }
  }

  /**
   * 提交：确保所有数据持久化到磁盘
   */
  public void commit() throws IOException {
    synchronized (this) {
      flushBuffer();
      fileChannel.force(true); // 强制刷盘，包括元数据
      committedPosition = fileChannel.position();
    }
  }

  /**
   * 读取从指定位置开始的记录
   */
  public List<byte[]> readFrom(long position) throws IOException {
    try (FileChannel readChannel = FileChannel.open(
        Paths.get(filePath), StandardOpenOption.READ)) {

      ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
      List<byte[]> records = new ArrayList<>();

      readChannel.position(position);

      while (readChannel.read(buffer) > 0) {
        buffer.flip();

        while (buffer.remaining() >= 8) {
          int magic = buffer.getInt();
          if (magic != 0x57414C31) {
            // 记录不完整或损坏
            break;
          }

          int length = buffer.getInt();
          if (buffer.remaining() < length) {
            // 数据不完整
            buffer.position(buffer.position() - 8); // 回退
            break;
          }

          byte[] record = new byte[length];
          buffer.get(record);
          records.add(record);
        }

        buffer.compact();
      }

      return records;
    }
  }

  private void flushBuffer() throws IOException {
    if (writeBuffer.position() > 0) {
      writeBuffer.flip();
      fileChannel.write(writeBuffer);
      writeBuffer.clear();
    }
  }

  public void close() throws IOException {
    flushBuffer();
    fileChannel.close();
  }
}
