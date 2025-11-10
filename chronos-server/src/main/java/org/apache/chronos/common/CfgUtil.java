package org.apache.chronos.common;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * Config load priority.
 * <p>
 * First: Config File of chronos.yml.
 * <p>
 * Second: System environment.
 * <p>
 * Three: System properties.
 * <p>
 * Four: Default Value
 */
public class CfgUtil {

  public static JsonObject readConfig() throws IOException {
    String cfgFile = getByEnvOrProperties(ChronosConfig.CFG_FILE);
    String content = IOUtils.resourceToString(cfgFile, StandardCharsets.UTF_8);
    return parseYamlToJson(content);
  }

  private static JsonObject parseYamlToJson(String yamlContent) {
    Yaml yaml = new Yaml();
    Map<String, Object> yamlMap = yaml.load(yamlContent);
    return JsonObject.mapFrom(yamlMap);
  }

  public static Integer getInteger(ChronosConfig key, JsonObject config) {
    String[] keys = key.getPropKey().split("\\.");
    JsonObject parent = getParent(config, keys);
    return parent.getInteger(keys[keys.length - 1], Integer.parseInt(getByEnvOrProperties(key)));
  }

  public static Boolean getBoolean(ChronosConfig key, JsonObject config) {
    String[] keys = key.getPropKey().split("\\.");
    JsonObject parent = getParent(config, keys);
    return parent.getBoolean(keys[keys.length - 1], Boolean.parseBoolean(getByEnvOrProperties(key)));
  }

  private static String getByEnvOrProperties(ChronosConfig key) {
    String value = System.getenv(key.getEnvKey());
    if (StringUtils.isEmpty(value)) {
      value = System.getProperty(key.getPropKey());
    }
    if (StringUtils.isEmpty(value)) {
      value = key.getDefaultValue();
    }
    return value;
  }

  public static JsonObject getParent(JsonObject config, String[] keys) {
    JsonObject result = config;
    if (keys.length > 1) {
      for (int i = 0; i < keys.length - 1; i++) {
        result = result.getJsonObject(keys[i]);
      }
    }
    return result;
  }
}
