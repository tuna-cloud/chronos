package org.apache.chronos.common;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

public class ConfigUtil {

  public static JsonObject readConfig() throws IOException {
    String cfgFile = System.getProperty("chronos.config", "/chronos.yaml");
    String content = IOUtils.resourceToString(cfgFile, StandardCharsets.UTF_8);
    return parseYamlToJson(content);
  }

  private static JsonObject parseYamlToJson(String yamlContent) {
    Yaml yaml = new Yaml();
    Map<String, Object> yamlMap = yaml.load(yamlContent);
    return JsonObject.mapFrom(yamlMap);
  }
}
