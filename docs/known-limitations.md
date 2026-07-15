# 已知限制与设计取舍（Known Limitations）

NotifyBus **Lite** 的定位是「在线变更触发广播」：只在消费者**在线时**告诉它「某个资源变了，自己去拉最新状态」。
它**故意不做**事件日志、cursor、幂等、重试、死信、顺序保证、DB。下面这些「限制」不是 bug，而是这个定位
的直接推论——列在这里是为了让使用方在选型时心里有数，并知道该由业务侧补什么。

如果你需要「不丢、可补发、严格有序、精确一次」，请用重量级的 NotifyBus（见仓库根 `CLAUDE.md` 描述的完整设计），
而不是 Lite。

---

## 1. Nacos topic 级订阅存在「丢失窗口」

### 现象

用 Nacos transport 时，**topic 级**订阅（`subscribeTopic` / `@NotifyBusListener(topic = "...")`，不带 `resourceKey`）
在「短时间内同一 topic 下多个资源连续变更」时，订阅方可能只收到**最后一次**，中间的触发被合并/覆盖掉。

**resource 级**订阅（带 `resourceKey`）不受此影响——每个 resourceKey 是独立的 dataId。

### 根因

Nacos 的 Config 模型是「一个 dataId 存一份内容，`publishConfig` 覆盖写、last-write-wins、没有队列」。
本 SDK 的 Nacos 实现里，**整个 topic 的所有 topic 级订阅者共用同一个 `broadcastDataId`**（见
`NacosNotifyBusTransport#subscribeTopic` → `NacosPaths.broadcastDataId()`）。发布时：

```
publishConfig(resourceDataId, resourceGroup, content)   // 给 resource 级订阅者，各自独立
publishConfig(broadcastDataId, broadcastGroup, content) // 给 topic 级订阅者，全 topic 共用一个 key
```

于是两次发布若发生在 Nacos 客户端一个长轮询周期之内，第二次 `publishConfig` 会把第一次的内容覆盖掉，
客户端下一次拉到的只是**最新值**，中间那次事件对 topic 级订阅者就「蒸发」了。ZooKeeper transport 也有类似
的「NodeCache 只反映最新值」特性，本质相同：**共享的最新值型通道天然会合并高频变更**。

```
t0  publish(topic=catalog, resourceKey=A, UPDATED)  -> broadcastDataId = {A,UPDATED,t0}
t1  publish(topic=catalog, resourceKey=B, UPDATED)  -> broadcastDataId = {B,UPDATED,t1}   // 覆盖
         ┌─────────────── 一个长轮询周期内 ───────────────┐
客户端下一次回调只看到 {B,UPDATED,t1}；{A,UPDATED,t0} 丢失。
```

### 为什么不修

「补上」意味着要给 topic 通道加序列号 + 事件日志 + cursor，让订阅方能发现「我漏了 t0」并回补——那正是重量级
NotifyBus 做的事，会把 Lite 变重。Lite 的契约是「触发是**提示**不是**命令**，丢了个别提示可接受」。

### 使用方怎么办

- **首选 resource 级订阅**：如果你真正关心的是「资源 A 变了」，直接 `subscribeResource(topic, "A", ...)`，
  A 的通道独立，不会被 B 的变更覆盖。
- **把 handler 写成幂等的「重新拉取」**：收到任意一次触发就去读**当前**最新全量/该资源最新状态。这样即使
  中间少收了几次，只要**最后**收到一次，最终状态依然正确（最终一致）。这也是本 SDK 一贯建议的消费姿势——
  事件不带 payload，本来就逼着你去拉最新。
- **对「必须每一次变更都感知」的场景**（如审计、计数）：Lite 不适合，请用重量级版本或直接上消息队列。

---

## 2. DELETED 语义偏弱

### 现象

`ChangeType.DELETED` 只是一个「提示：这个资源被删了」的**信号**，SDK 不保证：

- 消费者一定能把它和后续对**同一 resourceKey** 的 `CREATED` 区分出先后（见上面的合并/覆盖窗口）；
- DELETED 之后不会再来一条针对已删资源的旧事件（Lite 不做版本比较，不会把「陈旧事件」ACK 成 no-op）；
- 「删除」这个动作在传输通道上留痕——最新值型通道（Nacos config / ZK node）里，删除同样是「写一个
  内容为 DELETED 的最新值」，它随时会被下一次写覆盖。

### 根因

Lite 不携带业务数据、不存事件、不做版本/顺序仲裁。`changeType` 是**业务事实的提示**，不是 SDK 强制执行的
命令（这条原则对重量级版本也成立）。DELETED 的「弱」，本质是 Lite 不提供「删除的因果顺序保证」——而顺序
保证需要 sequence + cursor，正是被砍掉的部分。

### 使用方怎么办

- **别把 DELETED 当「执行删除」的命令**：把它当「去核对一下这个资源现在还在不在」的提示。handler 收到
  DELETED 后，**回源查真实状态**（DB / 配置中心里这条还在吗？），以**源**为准，而不是无脑本地删。
  这样即便 DELETED 和 CREATED 到达顺序错乱，最终以回源结果收敛。
- **删除后短期内避免复用同一个 resourceKey**：若删除后立刻用同名 key 新建，弱顺序会让消费者难以区分
  「旧的删」和「新的建」。业务侧给 key 加代际（如 `sku-123#v2`）能规避。
- **需要强删除语义**（删除必达、删除后拒绝旧事件）：用重量级 NotifyBus 的版本比较 + cursor，或在业务层
  用带版本号的软删除标记。

---

## 3. 其它由「在线触发」定位带来的取舍

| 限制 | 说明 | 业务侧对策 |
| --- | --- | --- |
| **不补发（no replay）** | 消费者离线期间的触发不会缓存，重连后**不会**收到期间的变更。 | 启动时先做一次全量 reload 兜底，再进入增量订阅。 |
| **过载丢弃（drop-on-saturation）** | 调度线程池队列（默认 1000）打满时，多余触发**直接丢弃并记 error 日志**，不阻塞、不无界堆积。 | handler 保持轻量+幂等；靠「拉最新」而非「逐条处理」收敛。必要时调大 `queue-capacity`/`max-pool-size`。 |
| **最终一致，非强一致** | 发布与「消费者收到」之间没有事务；发布方与消费方跨进程，只能保证最终一致。 | handler 幂等 + 回源，天然吸收乱序与丢失。 |
| **无精确一次（no exactly-once）** | 同一次变更可能触发 0 次、1 次或多次回调。 | handler 必须幂等——这是使用 Lite 的硬性前提。 |

---

## 一句话总结

Lite 的所有「弱」都指向同一个消费姿势：**收到任意触发 → 去拉当前最新状态 → 以源为准**。
只要 handler 是幂等的「重新拉取」，上面这些丢失/乱序/重复窗口对**最终状态**都无害。
如果你的场景无法接受「个别触发丢失」，那说明你需要的是重量级 NotifyBus，而不是 Lite。
