# CLAUDE.md — NotifyBus Lite

本文件指导 Claude Code 在 **notifybus-lite-parent** 仓库中工作。它覆盖任何默认行为，请严格遵守。

> 注意：仓库根 `../CLAUDE.md`（notify-bus 根目录那份）描述的是**重量级** NotifyBus 的设计，二者定位不同、
> 不要混淆。本目录是刻意做「减法」的 **Lite** 版本。凡是重量级那份要求的事件日志 / cursor / 幂等 / 重试 /
> 死信 / 顺序 / DB，在这里都是**明确不做**的（见「定位红线」）。

## NotifyBus Lite 是什么

轻量级「资源变更在线触发广播」SDK。发布方只说**「某个资源变了」**，消费方收到提示后**自己去拉最新状态**。
SDK 不搬运业务数据、不替消费方决定怎么刷新。核心消费姿势：**收到任意触发 → 拉当前最新状态 → 以源为准**；
只要 handler 幂等，个别触发的丢失/乱序/重复对最终状态无害——这正是 Lite 能砍掉 cursor/重试/幂等仍可用的前提。

## 定位红线（硬约束，不可违反）

以下方向一律**不实现**，也不要让设计朝它们漂移（需要这些能力的是重量级 NotifyBus，不是 Lite）：

- 事件日志 / cursor / 补发（replay）
- 幂等去重、重试、退避、死信队列
- 顺序保证（sequence 仲裁、严格有序）
- 内嵌 DB / 持久化 transport
- SDK 替消费方决定「怎么刷新」（失效 vs 重载 vs 重建，是消费方的事）
- 事件里塞业务 payload（事件只有 topic / resourceKey / changeType / timestamp）

如果一个改动会把 Lite「变重」，先停下来确认，而不是直接加。

## 模块结构与依赖方向

```
notifybus-lite-parent
├── notifybus-lite-core                  # 事件模型、发布/订阅门面、SPI 接口、调度线程池
├── notifybus-lite-transport-zk          # ZooKeeper transport（Curator）
├── notifybus-lite-transport-nacos       # Nacos transport（Nacos Config）
├── notifybus-lite-spring-boot-starter   # 自动装配、@NotifyBusListener 扫描、配置属性
├── notifybus-lite-test-support          # InMemoryNotifyBusTransport 等测试辅助
└── notifybus-lite-example               # plain-java quickstart + springboot 缓存刷新 demo
```

- **坐标/前缀**：groupId `com.tranbinlee.notifybus.lite`，基础包 `com.tranbinlee.notifybus.lite`，
  Spring 配置前缀 `notifybus.lite`，当前版本 `1.0.0-SNAPSHOT`，Spring Boot 2.7.18。
- **core 内部包依赖是硬规则**：`api → spi → event → exception`，无循环依赖。
  - `event`：`NotifyBusEvent`（不可变，Builder 构造，含 equals/hashCode）、`ChangeType`。
  - `spi`：`NotifyBusTransport`（publish + subscribeTopic + subscribeResource + close）、`NotifyBusSubscription`、
    `NotifyBusListener`（transport 回调契约，**放在 spi**——放 api 会造成 api↔spi 循环）。
  - `api`：`NotifyBusPublisher`、`NotifyBusConsumer`、`NotifyBusHandler`（面向业务的门面）。
  - `internal`：`NotifyBusDispatchExecutor`、`NamedDaemonThreadFactory`、`NoOpNotifyBusTransport`。
- **核心层不得依赖具体 transport**（ZK/Nacos 类不许进 core），**也不得依赖 Spring**（Spring 只在 starter）。

## 关键设计点

- **两种订阅粒度**：`subscribeTopic`（整个 topic）与 `subscribeResource`（某个 resourceKey）。
- **业务 handler 隔离**：回调统一切到 `NotifyBusDispatchExecutor` —— 命名、有界、可配置的线程池，
  **绝不**跑在 transport 自己的事件线程（如 ZK event thread）上；单个 handler 抛异常不影响其它事件。
- **过载安全降级**：调度队列（默认 1000）打满时 `LoggingDropPolicy` 丢弃并记 error 日志，不阻塞、不无界堆积。
  这与「在线触发、不保证补发」的定位一致。
- **NoOp 降级**：`notifybus.lite.enabled=false`（或缺省）时，starter 装配 `NoOpNotifyBusTransport`，
  发布不报错、订阅静默无回调。两个自动装配 (`NotifyBusAutoConfiguration` / `NotifyBusNoOpAutoConfiguration`)
  用 `@ConditionalOnProperty` 互斥。
- **注解扫描**：`@NotifyBusListener(topic=..., resourceKey=...)` 标注方法（签名固定 `void xxx(NotifyBusEvent)`），
  由 BeanPostProcessor 在 `afterSingletonsInstantiated()` 阶段统一订阅。
  （注意命名复用：starter 的 `@NotifyBusListener` **注解** vs core.spi 的 `NotifyBusListener` **接口**，是两个东西。）

## 编码约束（对本仓库生成的代码是硬约束）

1. **Java 8 语法**：不用 `record` / `var` / 模块系统等 Java 9+ 特性（源码与目标均 1.8）。
2. 所有 public API 需要完整 JavaDoc。
3. 线程池必须命名、可配置、优雅关闭；所有队列/循环必须有界。
4. `NotifyBusEvent` 不可变，经 Builder 构造并校验。
5. 业务 handler 调用必须 try/catch 兜底、计时、打统一结构日志（topic/resourceKey/changeType/timestamp/result/durationMs）。
6. 优先用 Builder（`NotifyBusConsumer.builder(transport)`）而非多参数构造器；旧五参构造器已 `@Deprecated`。
7. 核心层通过 SPI，不硬编码 ZK/Nacos 类。
8. 每个状态变更都要有单测覆盖。

## 构建与测试

- 全量：`mvn clean install`
- 单模块：`mvn -pl notifybus-lite-core test`
- 本机没有真实注册中心时，用 `InMemoryNotifyBusTransport`（core 测试里有一份包内可见的最小复制，
  避免 core→test-support→core 的模块环）或内嵌 ZK（`curator-test` 的 `TestingServer`，仅测试/示例用）测试。

> **本机没有 `mvn`**：静态验证靠 grep/扫描脚本（检查 import 是否解析、有无残留 FQN、依赖方向是否成立），
> 不做真实编译。改完包结构后务必跑一遍「每个文件用到的类型 ↔ package/import/FQN」的交叉检查。

## 已知限制与取舍

Lite 的有意取舍集中在 `docs/known-limitations.md`：**Nacos topic 级订阅丢失窗口**（topic 级订阅者共用一个
最新值型 broadcast 通道，高频变更会被覆盖合并——resource 级不受影响）、**DELETED 弱语义**（changeType 是提示
非命令，无版本/顺序仲裁）、不补发、过载丢弃、最终一致而非精确一次。改动涉及新限制时同步更新该文档。
