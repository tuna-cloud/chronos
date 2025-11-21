# 启动命令 JVM参数
```java
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/sun.nio=ALL-UNNAMED
--add-opens java.management/sun.management=ALL-UNNAMED
--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
-Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.Log4j2LogDelegateFactory
```

```page
Page size is 128 KB.
----------- block header(32 bytes)-------|------ page1 ------|------- page2 ---------| 
|--- magic value ---|--- writer index ---|---
|--- 4 bytes -------|--- 4 bytes --------|---
-----------------------------------------|-------------------|-----------------------|
```