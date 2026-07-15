# NotifyBus Lite Examples

两个可运行的最小示例，演示 NotifyBus Lite 的两种接入方式：**不依赖 Spring 的纯 Java 用法**，以及
**通过 `notifybus-lite-spring-boot-starter` 自动装配的 Spring Boot 用法**。两个示例共用同一个业务
场景：一个"商品目录变更"话题 `catalog.changed`，`resourceKey` 是 SKU（如 `sku-1001`）。发布方只广播
"变了"，从不携带商品数据本身；消费方收到触发信号后，自己去（模拟的）"源头"拉取最新状态，这正是
NotifyBus（含 Lite 版）反复强调的核心原则：**SDK 只负责告诉你"资源变了"，怎么应对是业务自己的事**。

```text
notifybus-lite-example/
├── notifybus-lite-example-plain-java/   # 非 Spring Boot：手动装配 Transport/Publisher/Consumer
└── notifybus-lite-example-springboot/   # Spring Boot：同一个进程既是发布方又是消费方
```

> Spring Boot 示例把发布方和消费方放进了**同一个进程**，纯粹是为了让示例开箱即跑、一条命令就能看到
> 完整闭环（curl 触发 → 同进程内的 `@NotifyBusListener` 立刻收到 → 缓存被刷新）。生产环境里发布方和
> 消费方几乎总是不同的服务，这里合并只是演示上的简化，SDK 本身对这一点没有任何限制或假设。

## 两种用法的对照

| | 非 Spring Boot（`plain-java`） | Spring Boot（`springboot`） |
|---|---|---|
| 依赖 | `notifybus-lite-core` + `notifybus-lite-transport-zk` | `notifybus-lite-spring-boot-starter` |
| 组装 `NotifyBusTransport` | 手动 `ZkNotifyBusTransportBuilder.builder()...build()` | starter 的 `NotifyBusAutoConfiguration` 根据 `application.yml` 自动装配好一个 `NotifyBusTransport` bean |
| 发布事件 | 手动 `new NotifyBusPublisher(transport)`，调用 `publish(...)` | 直接 `@Autowired NotifyBusPublisher`，装配已由 starter 完成 |
| 订阅事件 | 手动 `new NotifyBusConsumer(transport)`，调用 `subscribeTopic`/`subscribeResource`，拿到 `NotifyBusSubscription` 自己管理生命周期 | 用 `@NotifyBusListener(topic = ..., resourceKey = ...)` 标注方法，`NotifyBusListenerBeanPostProcessor` 在容器启动完成后自动扫描注册，容器关闭时自动退订 |
| 日志实现 | 需要自己引入一个 SLF4J binding（这里用 `slf4j-simple`），Spring Boot 环境不需要操心这个 | Spring Boot 自带 Logback |
| 生命周期管理 | 需要自己在 `finally` 里按 `NotifyBusSubscription -> NotifyBusConsumer -> NotifyBusTransport` 的顺序依次 `close()` | starter 用 `DisposableBean`/容器生命周期钩子自动收尾 |

结论：`notifybus-lite-core`/`notifybus-lite-transport-zk` 本身完全不依赖 Spring，纯 Java 项目可以直接
用；Spring Boot 只是把"装配 + 生命周期管理 + 注解扫描"这部分做成了自动化，底层用的是同一套 SPI。

## 1. 非 Spring Boot：`notifybus-lite-example-plain-java`

`PlainJavaQuickStartDemo` 是一个自包含的 `main()`，内嵌一个 Curator `TestingServer` 当作"零外部依赖"
的 ZooKeeper，跑起来不需要你自己先装一个 ZK 集群：

```bash
mvn -pl notifybus-lite-example/notifybus-lite-example-plain-java -am exec:java
```

预期输出：先看到内嵌 ZK 启动的连接串，然后发布 2 条事件（`sku-1001` 一条 UPDATED，`sku-2002` 一条
CREATED），最后打印一行 SUCCESS 总结——topic 级订阅收到 2 条，resourceKey 级订阅（只订阅
`sku-1001`）只收到 1 条，最后整个流程优雅关闭、无残留异常。

> ⚠️ 该模块的 `curator-test` 依赖（内嵌 `TestingServer`）**只用于本 demo**，方便零基础设施跑通整个
> 流程。生产代码永远不要依赖 `curator-test`，应该直接把 `connectString` 指向你真实的 ZooKeeper 集群。

## 2. Spring Boot：`notifybus-lite-example-springboot`

同一个进程里既有 REST 接口触发发布，又有 `@NotifyBusListener` 订阅并刷新本地缓存：

```bash
# 在 notifybus-lite-parent 目录下执行。先用 Docker Desktop 起一个本地 ZK（默认连接 127.0.0.1:2181），
# docker-compose.yml 在 notifybus-lite-example/ 目录下
docker compose -f notifybus-lite-example/docker-compose.yml up -d
docker compose -f notifybus-lite-example/docker-compose.yml logs -f zookeeper   # 看到 "binding to port 0.0.0.0/0.0.0.0:2181" 就说明起好了

mvn -pl notifybus-lite-example/notifybus-lite-example-springboot -am spring-boot:run
```

不想用 compose 也可以直接 `docker run --rm -p 2181:2181 zookeeper:3.8` 起一个一次性的（Ctrl+C 就没了，
不会残留容器）。

默认监听 `8081` 端口。启动后先看到 `[cache-state] (empty, waiting for the first trigger)` 的日志，
说明 `CacheStateReporter` 已经在跑（每 5 秒打印一次当前缓存快照）。

触发一次发布：

```bash
curl -X POST "http://127.0.0.1:8081/catalog/sku-1001/changes?changeType=UPDATED"
```

几秒内应该能在同一个控制台看到 `cache RELOAD sku=sku-1001 ...` 的日志——发布和消费在同一个进程，
不需要跨进程等待。

查看最近发布记录：

```bash
curl "http://127.0.0.1:8081/catalog/changes"
```

`CatalogCacheListener` 里演示了两种订阅粒度：一个方法只写 `topic`，监听这个 topic 下所有 SKU 的变化；
另一个方法额外指定 `resourceKey = "sku-1001"`，只关心这一个 SKU（发一条 `sku-2002` 的变更可以验证它
不会触发这个方法）。收到触发后，两个方法都不知道商品具体变成了什么样，都要靠调用
`CatalogSource.fetchLatest(sku)`（模拟的"回源"）才能拿到最新状态并刷新本地缓存——这也是 NotifyBus
Lite 不传输业务数据的直接体现。

也可以同时开两个该模块的实例（换个 `--server.port` 参数避免端口冲突），对照它们的日志确认两边各自
独立收到了同一次广播、最终缓存收敛一致——体现"每个订阅者都会拿到同一次广播"的语义。

## 故障排查

- **连不上 ZK**：确认 `application.yml` 里的 `notifybus.lite.zk.connect-string` 和实际起的 ZK 地址/端口一致；也可以用 `-Dnotifybus.lite.zk.connect-string=...` 启动参数覆盖。
- **收不到事件**：确认多开的实例之间 `notifybus.lite.namespace` 完全一致——不同 namespace 相当于两棵完全独立的 ZK 子树。
- **plain-java demo 报 `ClassNotFoundException`/日志没输出**：确认没有手动排除 `slf4j-simple`，非 Spring 环境下没有它就没有任何 SLF4J binding，日志会被丢弃或报 "no operation" 警告。
- **`notifybus.lite.enabled=false`（或没配置，即默认值）时调用 publish/subscribe 只打 WARN、不生效**：这是有意设计。`NotifyBusPublisher`/`NotifyBusConsumer` 在这种情况下依然可以正常注入和调用，不会因为 SDK 被关闭而导致业务服务启动失败；只是背后换成了一个空实现，每次调用 `publish`/`subscribeTopic`/`subscribeResource` 都会打印一条 WARN 日志提醒"SDK 未启用"，不会真正发布或收到任何事件。要让通知真正生效，把 `notifybus.lite.enabled` 显式设为 `true` 并配置好 `notifybus.lite.type`/`notifybus.lite.zk.*`。
