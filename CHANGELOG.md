# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [1.0.0] - 2026-07-15

首个正式发布版本。

### Added
- 核心事件模型（不可变 `NotifyBusEvent`，Builder 构造）、发布/订阅门面、SPI（`NotifyBusTransport`）。
- Transport 实现：ZooKeeper、Nacos，以及测试用 InMemory / NoOp。
- Spring Boot starter：自动装配、`@NotifyBusListener` 注解扫描、`notifybus.lite.enabled=false` 时降级为 NoOp。
- 纯 Java 与 Spring Boot 示例、README、`docs/known-limitations.md`（Nacos topic 级订阅丢失窗口、DELETED 弱语义等取舍）。
- `NotifyBusEvent` 的 `equals()` / `hashCode()`（基于 topic/resourceKey/changeType/timestamp 的值相等语义）。
- `NotifyBusPublisher` 支持注入 `java.time.Clock`，便于测试固定时间戳。
- `NotifyBusConsumer.builder(transport)` 链式构造器（可配置线程池前缀、core/max 线程数、队列容量、关闭等待秒数）。
- 开源工程文件：LICENSE（Apache-2.0）、CONTRIBUTING、本 CHANGELOG、`.gitignore`、`.gitattributes`、GitHub Actions CI。
- 测试：调度线程池过载丢弃 / 优雅关闭 / 强制中断、并发发布全量投递、Builder 线程名前缀、Clock 注入、事件 equals/hashCode。

### Deprecated
- `NotifyBusConsumer(transport, poolName, core, max, queue)` 五参构造器，改用 `builder(transport)`。

[Unreleased]: https://github.com/TranbinLee/notifybus-lite/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/TranbinLee/notifybus-lite/releases/tag/v1.0.0
