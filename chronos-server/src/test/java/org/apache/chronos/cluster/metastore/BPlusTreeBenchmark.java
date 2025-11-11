package org.apache.chronos.cluster.metastore;

import java.io.IOException;
import java.util.Random;

/**
 * B+树性能基准测试
 */
public class BPlusTreeBenchmark {

  public static void main(String[] args) throws IOException {
    benchmarkInsert();
    benchmarkSearch();
    benchmarkRangeQuery();
    benchmarkMixedWorkload();
  }

  private static void benchmarkInsert() throws IOException {
    System.out.println("=== Insert Benchmark ===");

    String filePath = "benchmark_insert.dat";
    MemoryMappedBPlusTree tree = new MemoryMappedBPlusTree(filePath, 8);

    int numRecords = 100000;
    Random random = new Random(42);

    long startTime = System.currentTimeMillis();

    for (int i = 0; i < numRecords; i++) {
      long key = random.nextInt(1000000);
      tree.insert(key, "Value" + key);
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Inserted " + numRecords + " records in " + duration + "ms");
    System.out.println("Throughput: " + (numRecords * 1000.0 / duration) + " ops/sec");

    MemoryMappedBPlusTree.TreeStats stats = tree.getStats();
    System.out.println("Tree stats: " + stats);

    tree.close();
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(filePath));
  }

  private static void benchmarkSearch() throws IOException {
    System.out.println("\n=== Search Benchmark ===");

    String filePath = "benchmark_search.dat";
    MemoryMappedBPlusTree tree = new MemoryMappedBPlusTree(filePath, 8);

    int numRecords = 50000;
    Random random = new Random(42);
    long[] keys = new long[numRecords];

    // 预填充数据
    for (int i = 0; i < numRecords; i++) {
      keys[i] = random.nextInt(1000000);
      tree.insert(keys[i], "Value" + keys[i]);
    }

    // 搜索测试
    long startTime = System.currentTimeMillis();

    int found = 0;
    for (int i = 0; i < numRecords; i++) {
      if (tree.search(keys[i]) != null) {
        found++;
      }
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Searched " + numRecords + " records in " + duration + "ms");
    System.out.println("Throughput: " + (numRecords * 1000.0 / duration) + " ops/sec");
    System.out.println("Found: " + found + "/" + numRecords);

    tree.close();
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(filePath));
  }

  private static void benchmarkRangeQuery() throws IOException {
    System.out.println("\n=== Range Query Benchmark ===");

    String filePath = "benchmark_range.dat";
    MemoryMappedBPlusTree tree = new MemoryMappedBPlusTree(filePath, 8);

    int numRecords = 100000;

    // 预填充顺序数据
    for (long i = 0; i < numRecords; i++) {
      tree.insert(i, "Value" + i);
    }

    // 范围查询测试
    int numQueries = 1000;
    long totalResults = 0;

    long startTime = System.currentTimeMillis();

    Random random = new Random(42);
    for (int i = 0; i < numQueries; i++) {
      long startKey = random.nextInt(numRecords - 1000);
      long endKey = startKey + 100;
      java.util.List<String> results = tree.rangeSearch(startKey, endKey);
      totalResults += results.size();
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Executed " + numQueries + " range queries in " + duration + "ms");
    System.out.println("Throughput: " + (numQueries * 1000.0 / duration) + " queries/sec");
    System.out.println("Average results per query: " + (totalResults / (double) numQueries));

    tree.close();
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(filePath));
  }

  private static void benchmarkMixedWorkload() throws IOException {
    System.out.println("\n=== Mixed Workload Benchmark ===");

    String filePath = "benchmark_mixed.dat";
    MemoryMappedBPlusTree tree = new MemoryMappedBPlusTree(filePath, 8);

    int numOperations = 100000;
    Random random = new Random(42);

    long startTime = System.currentTimeMillis();

    for (int i = 0; i < numOperations; i++) {
      int operation = random.nextInt(100);
      long key = random.nextInt(10000);

      if (operation < 60) { // 60% 插入
        tree.insert(key, "Value" + key);
      } else if (operation < 90) { // 30% 搜索
        tree.search(key);
      } else { // 10% 删除
        tree.delete(key);
      }
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    System.out.println("Completed " + numOperations + " mixed operations in " + duration + "ms");
    System.out.println("Throughput: " + (numOperations * 1000.0 / duration) + " ops/sec");

    MemoryMappedBPlusTree.TreeStats stats = tree.getStats();
    System.out.println("Final tree stats: " + stats);

    tree.close();
    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(filePath));
  }
}
