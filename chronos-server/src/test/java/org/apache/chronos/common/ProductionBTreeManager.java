package org.apache.chronos.common;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class ProductionBTreeManager {

  private final DB db;
  private final BTreeMap<String, Integer> codeToIdMap;
  private final AtomicLong operationCounter = new AtomicLong(0);
  private final ScheduledExecutorService maintenanceExecutor;
  private final File dbFile;

  // 监控指标
  private final AtomicLong readCount = new AtomicLong(0);
  private final AtomicLong writeCount = new AtomicLong(0);
  private final AtomicLong errorCount = new AtomicLong(0);

  public ProductionBTreeManager(String dbPath) {
    this.dbFile = new File(dbPath);
    validateDatabasePath(dbFile);

    this.db = createDatabase(dbFile);
    this.codeToIdMap = createBTreeMap();
    this.maintenanceExecutor = Executors.newScheduledThreadPool(2);
  }

  private void validateDatabasePath(File dbFile) {
    File parentDir = dbFile.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new RuntimeException("无法创建数据库目录: " + parentDir.getAbsolutePath());
      }
    }

    // 检查目录可写
    if (parentDir != null && !parentDir.canWrite()) {
      throw new RuntimeException("数据库目录不可写: " + parentDir.getAbsolutePath());
    }
  }

  private DB createDatabase(File dbFile) {
    try {
      return DBMaker
          .fileDB(dbFile)
          .fileMmapEnableIfSupported()
          .fileMmapPreclearDisable()
          .allocateStartSize(256 * 1024 * 1024) // 初始256MB
          .allocateIncrement(128 * 1024 * 1024) // 每次扩展128MB
          // 事务配置
          .transactionEnable()
          .closeOnJvmShutdown()
          // 并发配置
          .concurrencyScale(Runtime.getRuntime().availableProcessors() * 2)
          // 容错配置
          .checksumHeaderBypass()
          .make();
    } catch (Exception e) {
      throw new RuntimeException("数据库初始化失败: " + e.getMessage(), e);
    }
  }

  private BTreeMap<String, Integer> createBTreeMap() {
    try {
      return db
          .treeMap("codeToIdMapping")
          .keySerializer(Serializer.STRING)
          .valueSerializer(Serializer.INTEGER)

          // B+Tree 优化配置
          .valuesOutsideNodesEnable() // 值存储在节点外部
          .counterEnable() // 启用计数器
          .maxNodeSize(128) // 每个节点最多128个条目
          .createOrOpen();
    } catch (Exception e) {
      throw new RuntimeException("BTreeMap 初始化失败: " + e.getMessage(), e);
    }
  }
}
