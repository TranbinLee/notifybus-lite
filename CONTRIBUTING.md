# 贡献指南

感谢参与 NotifyBus Lite！在提 PR 之前，请花几分钟读一下本文，尤其是「定位红线」——它决定了什么样的改动
会被接受。

## 定位红线（先读这条）

NotifyBus Lite 是**在线变更触发广播**，不是消息队列，不是配置中心。以下方向的 PR **不会**被接受，因为它们
会把 Lite 变重、偏离定位（需要这些能力请用重量级 NotifyBus）：

- 事件日志 / cursor / 补发（replay）
- 幂等去重、重试、退避、死信队列
- 顺序保证（sequence 仲裁、严格有序）
- 内嵌 DB / 持久化 transport
- 让 SDK 替消费方决定「怎么刷新」（失效 vs 重载 vs 重建）
- 在事件里塞业务 payload

如果你不确定一个改动算不算「变重」，先开 issue 讨论再动手。

## 开发环境

- JDK 8（源码与字节码目标都是 1.8，**不要**使用 `record` / `var` / 模块系统等 Java 9+ 语法）
- Maven 3.6+

## 构建与测试

```bash
mvn clean install          # 全量构建 + 测试
mvn -pl notifybus-lite-core test   # 只测某个模块
```

提 PR 前请确保 `mvn clean install` 通过。

## 代码约定

- **依赖方向是硬规则**：`api → spi → event → exception`，无循环依赖；核心层不得依赖具体 transport
  （ZK/Nacos 类不允许出现在 core），也不得依赖 Spring（Spring 只在 starter）。
- **所有 public API 需要完整 JavaDoc。**
- **线程池必须命名、可配置、优雅关闭；所有队列/循环必须有界。**
- 业务 handler 调用必须 try/catch 兜底、计时、打统一结构日志。
- 每个状态变更都要有单测覆盖；本机没有真实注册中心时用 `InMemoryNotifyBusTransport` / 内嵌 ZK 测试。
- 保持与周围代码一致的命名、注释密度和风格。

## 提交与 PR

- commit message 用祈使句、简洁说明「做了什么、为什么」。
- 一个 PR 聚焦一件事；改动行为时同步更新 `CHANGELOG.md` 的 `Unreleased` 段。
- 涉及新限制/取舍时，更新 `docs/known-limitations.md`。

## 许可证

提交即表示你同意你的贡献以 [Apache-2.0](LICENSE) 授权发布。
