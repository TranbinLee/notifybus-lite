# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added
- `NotifyBusEvent` 增加 `equals()` / `hashCode()`（基于 topic/resourceKey/changeType/timestamp 的值相等语义）。
- `NotifyBusPublisher` 支持注入 `java.time.Clock`，便于测试固定时间戳。
- `NotifyBusConsumer` 新增 `builder(transport)` 链式构造器（可配置线程池前缀、core/max 线程数、队列容量、
  关闭等待秒数），替代多参数构造器。
- `docs/known-limitations.md`：集中记录 Lite 的设计取舍，含 Nacos topic 级订阅丢失窗口、DELETED 弱语义的详解。
- 仓库开源文件：README、LICENSE（Apache-2.0）、CONTRIBUTING、本 CHANGELOG、`.gitignore`、GitHub Actions CI。
- 新增测试：调度线程池过载丢弃 / 优雅关闭 / 强制中断、并发发布全量投递、Builder 线程名前缀、Clock 注入、
  事件 equals/hashCode。

### Changed
- Spring Boot starter 的两个自动装配改用 `NotifyBusConsumer.builder(...)` 构造 consumer。

### Deprecated
- `NotifyBusConsumer(transport, poolName, core, max, queue)` 五参构造器，改用 `builder(transport)`。

## [1.0.0-SNAPSHOT]

初始版本：核心事件模型、发布/订阅门面、SPI，ZooKeeper / Nacos / InMemory / NoOp transport，
Spring Boot starter 与 `@NotifyBusListener` 注解扫描，纯 Java 与 Spring Boot 示例。
