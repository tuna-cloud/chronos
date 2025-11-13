package org.apache.chronos.cluster.metastore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WriteAheadLog {

  private static final Logger log = LogManager.getLogger(WriteAheadLog.class);
  /**
   * Just record all meta data id. All MetaData Follower update its meta data by metaDataId
   */
  private final FileChannel fileChannel;
  private final ByteBuffer writeBuffer;
  private final String filePath;

  public WriteAheadLog(String filePath, int bufferSize) throws IOException {
    this.filePath = filePath;
    this.fileChannel = FileChannel.open(
        Paths.get(filePath),
        StandardOpenOption.CREATE,
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
      if (!writeBuffer.hasRemaining()) {
        commit();
      }
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
      }
    }
  }

  /**
   * 读取从指定位置开始的记录
   */
  public ByteBuf readFrom(long position, int maxLength) throws IOException {
    try (FileChannel readChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
      readChannel.position(position);
      int size = (int) (fileChannel.size() - position);
      size = Math.min(size, maxLength);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      readChannel.read(buffer, position);
      buffer.flip();
      return Unpooled.wrappedBuffer(buffer);
    }
  }

  public void truncateTo(long endPosition) {
    try (RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        FileChannel channel = file.getChannel()) {
      ByteBuffer buffer = ByteBuffer.allocate((int) (channel.size() - endPosition));
      channel.position(endPosition);
      channel.read(buffer);
      channel.truncate(0);
      buffer.flip();
      channel.write(buffer);
    } catch (Exception e) {
      log.error("truncate err", e);
    }
  }

  public void close() throws IOException {
    commit();
    fileChannel.close();
  }
}
