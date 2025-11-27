package org.apache.chronos.common;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.codec.digest.XXHash32;

public class HashTest {

  public static void main(String[] args) {
    int max = 400_0000;
    int valueMax = 100_0000;
    Map<Integer, Integer> map = Maps.newHashMapWithExpectedSize(max);
    Map<Integer, String> map1 = Maps.newHashMapWithExpectedSize(max);
    for (int i = 0; i < valueMax; i++) {
      String msg = "test_" + i;
      XXHash32 xxHash32 = new XXHash32(0);
      xxHash32.update(msg.getBytes(), 0, msg.getBytes().length);
      long value = xxHash32.getValue();
      if (value > 0xFFFFFFFFL) {
        System.out.println("123123");
      }
      int idx = (int) (value % max);
      if (map.containsKey(idx)) {
        System.out.println(map1.get(idx) + ":" + msg);
        map.put(idx, map.get(idx) + 1);
      } else {
        map.put(idx, 1);
      }
      map1.put(idx, msg);
    }
    int count1 = 0;
    int count2 = 0;
    int count3 = 0;
    int count4 = 0;
    int count5 = 0;
    int count6 = 0;
    int count7 = 0;
    int count8 = 0;
    int count9 = 0;
    for (Entry<Integer, Integer> entry : map.entrySet()) {
      if (entry.getValue() > 1) {
        count1 ++;
      }
      if (entry.getValue() > 2) {
        count2 ++;
      }
      if (entry.getValue() > 3) {
        count3 ++;
      }
      if (entry.getValue() > 4) {
        count4 ++;
      }
      if (entry.getValue() > 5) {
        count5 ++;
      }
      if (entry.getValue() > 6) {
        count6 ++;
      }
      if (entry.getValue() > 7) {
        count7 ++;
      }
      if (entry.getValue() > 8) {
        count8 ++;
      }
      if (entry.getValue() > 9) {
        count9 ++;
      }
    }
    System.out.println("size: " + map.size());
    System.out.println("count1: " + count1);
    System.out.println("count2: " + count2);
    System.out.println("count3: " + count3);
    System.out.println("count4: " + count4);
    System.out.println("count5: " + count5);
    System.out.println("count6: " + count6);
    System.out.println("count7: " + count7);
    System.out.println("count8: " + count8);
    System.out.println("count9: " + count9);
  }
}
