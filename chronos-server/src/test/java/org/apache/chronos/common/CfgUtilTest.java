package org.apache.chronos.common;


import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CfgUtilTest {


  @Test
  public void testGetParent() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.put("test", new JsonObject().put("value", "V"));

    JsonObject parent = CfgUtil.getParent(jsonObject, "test.value".split("\\."));
    Assertions.assertTrue(parent.containsKey("value"));

    JsonObject parent1 = CfgUtil.getParent(jsonObject, "testValue".split("\\."));
    Assertions.assertTrue(parent1.containsKey("test"));
  }
}