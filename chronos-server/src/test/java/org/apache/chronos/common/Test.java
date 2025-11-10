package org.apache.chronos.common;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

public class Test {

  public static void main(String[] args) {
    RoaringBitmap bitmap1 = new RoaringBitmap();
    RoaringBitmap bitmap2 = new RoaringBitmap();

    // 添加数据
    bitmap1.add(1, 2, 3, 1000, 1001, 1000000);
    bitmap2.add(2, 3, 4, 1001, 1002);

    // 集合运算
    RoaringBitmap and = RoaringBitmap.and(bitmap1, bitmap2);
    RoaringBitmap or = RoaringBitmap.or(bitmap1, bitmap2);

    System.out.println("AND: " + and);
    System.out.println("OR: " + or);
    System.out.println("Cardinality: " + and.getCardinality());

    // 迭代
    IntIterator iterator = and.getIntIterator();
    while (iterator.hasNext()) {
      System.out.println("Record ID: " + iterator.next());
    }
  }
}
