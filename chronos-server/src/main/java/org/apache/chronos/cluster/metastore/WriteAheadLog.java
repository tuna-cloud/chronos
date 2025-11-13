package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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
        StandardOpenOption.APPEND
    );

    this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);
  }

  /**
   * 写入WAL记录
   *
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
      if (writeBuffer.position() > 0) {
        writeBuffer.flip();
        fileChannel.write(writeBuffer);
        writeBuffer.clear();
        fileChannel.force(true); // 强制刷盘，包括元数据
        committedPosition = fileChannel.position();
      }
    }
  }

  /**
   * 读取从指定位置开始的记录
   */
  public ByteBuf readFrom(long position) throws IOException {
    try (FileChannel readChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {

      readChannel.position(position);
      ByteBuffer buffer = ByteBuffer.allocate((int) (fileChannel.size() - position));
      int reads = readChannel.read(buffer);
      buffer.flip();
      return Unpooled.wrappedBuffer(buffer);
    }
  }

  public void close() throws IOException {
    commit();
    fileChannel.close();
  }
}
