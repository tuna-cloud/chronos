package org.apache.chronos.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import java.io.IOException;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

public class Test {

  public static void main(String[] args) throws IOException {
    RoaringBitmap bitmap1 = new RoaringBitmap();
    RoaringBitmap bitmap2 = new RoaringBitmap();

    System.out.println("size: " + bitmap2.getSizeInBytes());
    // 添加数据
    bitmap1.add(1, 2, 3, 1000, 1001, 1000000);
    for (int i = 0; i < 100_0000; i++) {
      bitmap2.add(i);
    }
    System.out.println("size: " + bitmap2.getSizeInBytes() / 1024);

    // 集合运算
    RoaringBitmap and = RoaringBitmap.and(bitmap1, bitmap2);
    RoaringBitmap or = RoaringBitmap.or(bitmap1, bitmap2);

    System.out.println("AND: " + and);
    System.out.println("Cardinality: " + and.getCardinality());
    System.out.println("Or Cardinality: " + or.getCardinality());

    and.serialize(new ByteBufOutputStream(ByteBufAllocator.DEFAULT.buffer()));
    // 迭代
    IntIterator iterator = and.getIntIterator();
    while (iterator.hasNext()) {
      System.out.println("Record ID: " + iterator.next());
    }

  }
}
