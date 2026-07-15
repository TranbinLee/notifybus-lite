# NotifyBus Lite

轻量级「资源变更在线触发广播」SDK。发布方只说**「某个资源变了」**，消费方收到提示后**自己去拉最新状态**——
SDK 不搬运业务数据、不替消费方决定怎么刷新（失效缓存 / 重载 / 重建索引，都是消费方自己的事）。

> **一句话定位**：在线时把「变了」这个信号广播出去，仅此而已。
> 它**故意不做**事件日志、cursor、幂等、重试、死信、顺序保证、DB。需要「不丢、可补发、精确一次、严格有序」的场景，
> 请用重量级的 NotifyBus，而不是 Lite。详见 [docs/known-limitations.md](docs/known-limitations.md)。

## 特性

- **两种订阅粒度**：topic 级（该 topic 下任何资源变化都收）与 resource 级（只关心某个 `resourceKey`）。
- **可插拔 transport（SPI）**：内置 ZooKeeper、Nacos 实现，以及测试用的 InMemory / NoOp，通过单一
  `NotifyBusTransport` 接口扩展。
- **业务 handler 隔离**：回调统一切到独立、命名、有界的调度线程池执行，绝不跑在 transport 自己的事件线程上；
  单个 handler 抛异常不影响其它事件。
- **过载安全降级**：调度队列打满时丢弃并记日志，不阻塞、不无界堆积。
- **Spring Boot 开箱即用**：`@NotifyBusListener` 注解扫描 + 自动装配；`notifybus.lite.enabled=false` 时自动降级为
  NoOp（发布不报错、订阅静默无回调），方便本地/测试环境一键关闭。
- **Java 8 兼容**，无 Spring 依赖侵入核心（Spring 只在 starter 里）。

## 模块结构

| 模块 | 职责 |
| --- | --- |
| `notifybus-lite-core` | 核心：事件模型、发布/订阅门面、SPI 接口、调度线程池。`api → spi → event → exception`，无循环依赖 |
| `notifybus-lite-transport-zk` | ZooKeeper transport（Curator） |
| `notifybus-lite-transport-nacos` | Nacos transport（Nacos Config） |
| `notifybus-lite-spring-boot-starter` | 自动装配、`@NotifyBusListener` 扫描、配置属性 |
| `notifybus-lite-test-support` | `InMemoryNotifyBusTransport` 等测试辅助 |
| `notifybus-lite-example` | 可运行示例：纯 Java quickstart + Spring Boot 缓存刷新 demo |

坐标：`com.tranbinlee.notifybus.lite`，当前版本 `1.0.0`，Spring 配置前缀 `notifybus.lite`。

## 快速开始

### 构建

```bash
mvn clean install
```

### 纯 Java 接入（不依赖 Spring）

```java
// 1. 构造 transport（生产指向真实 ZK/Nacos 集群）
NotifyBusTransport transport = ZkNotifyBusTransportBuilder.builder()
        .connectString("zk-host:2181")
        .rootPath("/notifybus-lite")
        .build();

// 2. 由 transport 派生发布/订阅门面
NotifyBusPublisher publisher = new NotifyBusPublisher(transport);
NotifyBusConsumer consumer = NotifyBusConsumer.builder(transport).build();

// 3a. topic 级订阅：该 topic 下任何资源变化都收到
NotifyBusSubscription topicSub = consumer.subscribeTopic("catalog.changed", event -> {
    // 收到提示后回源拉最新状态，handler 保持幂等
    reloadCatalog();
});

// 3b. resource 级订阅：只关心某个具体资源
NotifyBusSubscription skuSub = consumer.subscribeResource("catalog.changed", "sku-1001", event -> {
    refreshSku("sku-1001");
});

// 4. 发布一次变更提示（不带业务 payload，只有 changeType）
publisher.publish("catalog.changed", "sku-1001", ChangeType.UPDATED);

// 5. 逆序优雅关闭：取消订阅 -> 关调度线程池 -> 关 transport
topicSub.close();
skuSub.close();
consumer.close();      // 关闭调度线程池
transport.close();     // 关闭 ZK 连接
```

完整可运行版本见 `notifybus-lite-example/notifybus-lite-example-plain-java`（内嵌 ZK，`mvn -pl ... exec:java` 或直接跑 `main`）。

### Spring Boot 接入

引入 starter 与一个 transport：

```xml
<dependency>
    <groupId>com.tranbinlee.notifybus.lite</groupId>
    <artifactId>notifybus-lite-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
<dependency>
    <groupId>com.tranbinlee.notifybus.lite</groupId>
    <artifactId>notifybus-lite-transport-zk</artifactId>
    <version>1.0.0</version>
</dependency>
```

配置：

```yaml
notifybus:
  lite:
    enabled: true              # false 或缺省 -> 自动降级为 NoOp
    type: zookeeper            # 或 nacos
    namespace: /notifybus
    zk:
      connect-string: zk-host:2181
    dispatch:
      thread-name-prefix: notifybus-lite-dispatch
      core-pool-size: 2
      max-pool-size: 8
      queue-capacity: 1000
```

订阅：在任意 bean 的方法上标注 `@NotifyBusListener`，方法签名固定 `void xxx(NotifyBusEvent event)`：

```java
@Component
public class CatalogCacheListener {

    @NotifyBusListener(topic = "catalog.changed")            // topic 级
    public void onAnyChange(NotifyBusEvent event) {
        reloadCatalog();
    }

    @NotifyBusListener(topic = "catalog.changed", resourceKey = "sku-1001")  // resource 级
    public void onOneSku(NotifyBusEvent event) {
        refreshSku("sku-1001");
    }
}
```

发布：注入 `NotifyBusPublisher` 直接 `publish(...)`。完整示例见
`notifybus-lite-example/notifybus-lite-example-springboot`。

## 消费姿势（重要）

事件**不带业务 payload**，只有 `topic` / `resourceKey` / `changeType` / `timestamp`。正确的 handler 写法是：

> **收到任意触发 → 去拉当前最新状态 → 以源为准。**

只要 handler 是幂等的「重新拉取」，个别触发的丢失 / 乱序 / 重复对**最终状态**都无害——这也是 Lite 能砍掉
cursor/重试/幂等仍然可用的前提。`changeType` 和 `DELETED` 都只是**提示**不是命令，细节与边界见
[docs/known-limitations.md](docs/known-limitations.md)。

## 已知限制

Lite 有意的取舍（不补发、过载丢弃、Nacos topic 级合并窗口、DELETED 弱语义、最终一致而非精确一次）集中记录在
[docs/known-limitations.md](docs/known-limitations.md)。选型前请读一遍。

## 开发

- 构建全部模块：`mvn clean install`
- 单模块测试：`mvn -pl notifybus-lite-core test`
- Java 8 语法约束：不使用 `record` / `var` / 模块系统等 Java 9+ 特性。

## 许可证

[Apache License 2.0](LICENSE)。
