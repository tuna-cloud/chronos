package org.apache.chronos.cluster.metastore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MemoryMappedBPlusTreeTest {

  @TempDir
  Path tempDir;

  private MemoryMappedBPlusTree tree;
  private String testFilePath;

  @BeforeEach
  void setUp() throws IOException {
    testFilePath = tempDir.resolve("test_bplus_tree.dat").toString();
    tree = new MemoryMappedBPlusTree(testFilePath, 4);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tree != null) {
      tree.close();
    }
  }

  @Test
  void testBasicInsertAndSearch() throws IOException {
    tree.insert(10L, "Value10");
    tree.insert(20L, "Value20");
    tree.insert(5L, "Value5");
    tree.insert(15L, "Value15");

    assertEquals("Value10", tree.search(10L));
    assertEquals("Value20", tree.search(20L));
    assertEquals("Value5", tree.search(5L));
    assertEquals("Value15", tree.search(15L));
    assertNull(tree.search(100L));
  }

  @Test
  void testDuplicateKeyUpdate() throws IOException {
    tree.insert(10L, "InitialValue");
    tree.insert(10L, "UpdatedValue");

    assertEquals("UpdatedValue", tree.search(10L));
  }

  @Test
  void testRangeSearch() throws IOException {
    for (long i = 1; i <= 100; i++) {
      tree.insert(i, "Value" + i);
    }

    List<String> results = tree.rangeSearch(25L, 35L);
    assertEquals(11, results.size());
    assertEquals("Value25", results.get(0));
    assertEquals("Value35", results.get(10));
  }

  @Test
  void testRangeSearchEmpty() throws IOException {
    List<String> results = tree.rangeSearch(100L, 200L);
    assertTrue(results.isEmpty());
  }

  @Test
  void testDelete() throws IOException {
    tree.insert(10L, "Value10");
    tree.insert(20L, "Value20");
    tree.insert(30L, "Value30");

    assertTrue(tree.delete(20L));
    assertNull(tree.search(20L));
    assertEquals("Value10", tree.search(10L));
    assertEquals("Value30", tree.search(30L));

    assertFalse(tree.delete(999L));
  }

  @Test
  void testMultipleDeletes() throws IOException {
    for (long i = 1; i <= 50; i++) {
      tree.insert(i, "Value" + i);
    }

    for (long i = 10; i <= 20; i++) {
      assertTrue(tree.delete(i));
    }

    for (long i = 10; i <= 20; i++) {
      assertNull(tree.search(i));
    }

    for (long i = 1; i <= 9; i++) {
      assertEquals("Value" + i, tree.search(i));
    }

    for (long i = 21; i <= 50; i++) {
      assertEquals("Value" + i, tree.search(i));
    }
  }

  @Test
  void testTreeStructureAfterInserts() throws IOException {
    for (long i = 1; i <= 20; i++) {
      tree.insert(i, "Value" + i);
    }

    MemoryMappedBPlusTree.TreeStats stats = tree.getStats();
    assertTrue(stats.totalNodes > 0);
    assertTrue(stats.leafNodes > 0);
    assertTrue(stats.internalNodes >= 0);
    assertEquals(20, stats.totalKeys);

    for (long i = 1; i <= 20; i++) {
      assertEquals("Value" + i, tree.search(i));
    }
  }

  @Test
  void testSequentialInsertAndSearch() throws IOException {
    int count = 1000;

    long startTime = System.currentTimeMillis();
    for (long i = 0; i < count; i++) {
      tree.insert(i, "Seq" + i);
    }
    long insertTime = System.currentTimeMillis() - startTime;

    startTime = System.currentTimeMillis();
    for (long i = 0; i < count; i++) {
      assertEquals("Seq" + i, tree.search(i));
    }
    long searchTime = System.currentTimeMillis() - startTime;

    System.out.println("Sequential - Insert: " + insertTime + "ms, Search: " + searchTime + "ms");

    MemoryMappedBPlusTree.TreeStats stats = tree.getStats();
    System.out.println("Stats: " + stats);
  }

  @Test
  void testReverseSequentialInsert() throws IOException {
    int count = 1000;
    for (long i = count - 1; i >= 0; i--) {
      tree.insert(i, "Rev" + i);
    }

    for (long i = 0; i < count; i++) {
      assertEquals("Rev" + i, tree.search(i));
    }
  }

  @Test
  void testRandomInsertAndSearch() throws IOException {
    int count = 1000;
    long[] keys = new long[count];

    for (int i = 0; i < count; i++) {
      keys[i] = (long) (Math.random() * 10000);
      tree.insert(keys[i], "Random" + keys[i]);
    }

    int found = 0;
    for (int i = 0; i < count; i++) {
      if (tree.search(keys[i]) != null) {
        found++;
      }
    }

    assertEquals(count, found, "All inserted keys should be found");
  }

  @Test
  void testTreePrint() throws IOException {
    for (long i = 1; i <= 15; i++) {
      tree.insert(i, "Value" + i);
    }

    tree.printTree();
    MemoryMappedBPlusTree.TreeStats stats = tree.getStats();
    System.out.println("Stats: " + stats);
  }

  @Test
  void testPersistence() throws IOException {
    // 插入数据
    for (long i = 1; i <= 100; i++) {
      tree.insert(i, "Persist" + i);
    }

    // 关闭树
    tree.close();

    // 重新打开树
    tree = new MemoryMappedBPlusTree(testFilePath, 4);

    // 验证数据仍然存在
    for (long i = 1; i <= 100; i++) {
      assertEquals("Persist" + i, tree.search(i));
    }
  }

  @Test
  void testEdgeCases() throws IOException {
    // 测试边界值
    tree.insert(Long.MAX_VALUE, "MaxValue");
    tree.insert(Long.MIN_VALUE, "MinValue");
    tree.insert(0L, "ZeroValue");

    assertEquals("MaxValue", tree.search(Long.MAX_VALUE));
    assertEquals("MinValue", tree.search(Long.MIN_VALUE));
    assertEquals("ZeroValue", tree.search(0L));

    // 空树操作
    MemoryMappedBPlusTree emptyTree = new MemoryMappedBPlusTree(
        tempDir.resolve("empty_tree.dat").toString());
    assertNull(emptyTree.search(1L));
    assertTrue(emptyTree.rangeSearch(1L, 10L).isEmpty());
    assertFalse(emptyTree.delete(1L));
    emptyTree.close();
  }

  @Test
  void testLargeDataset() throws IOException {
    int datasetSize = 5000;

    System.out.println("Testing with " + datasetSize + " records...");

    long startTime = System.currentTimeMillis();

    // 插入
    for (long i = 0; i < datasetSize; i++) {
      tree.insert(i, "Data" + i);
    }

    long insertTime = System.currentTimeMillis() - startTime;

    // 搜索
    startTime = System.currentTimeMillis();
    for (long i = 0; i < datasetSize; i++) {
      String result = tree.search(i);
      assertNotNull(result);
      assertEquals("Data" + i, result);
    }
    long searchTime = System.currentTimeMillis() - startTime;

    // 范围查询
    startTime = System.currentTimeMillis();
    List<String> rangeResults = tree.rangeSearch(datasetSize / 4, datasetSize / 2);
    long rangeTime = System.currentTimeMillis() - startTime;

    // 删除
    startTime = System.currentTimeMillis();
    for (long i = 0; i < datasetSize / 2; i++) {
      assertTrue(tree.delete(i));
    }
    long deleteTime = System.currentTimeMillis() - startTime;

    System.out.println("Large dataset performance:");
    System.out.println("Insert: " + insertTime + "ms");
    System.out.println("Search: " + searchTime + "ms");
    System.out.println("Range: " + rangeTime + "ms");
    System.out.println("Delete: " + deleteTime + "ms");

    MemoryMappedBPlusTree.TreeStats stats = tree.getStats();
    System.out.println("Final stats: " + stats);

    assertTrue(insertTime < 1000, "Insert should be fast");
    assertTrue(searchTime < 500, "Search should be fast");
  }
}