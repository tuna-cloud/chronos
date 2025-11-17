# 启动命令 JVM参数
```java
--add-modules
java.se
--add-exports
java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens
java.base/java.lang=ALL-UNNAMED
--add-opens
java.base/sun.nio.ch=ALL-UNNAMED
--add-opens
java.management/sun.management=ALL-UNNAMED
--add-opens
jdk.management/com.sun.management.internal=ALL-UNNAMED
-Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.Log4j2LogDelegateFactory
```