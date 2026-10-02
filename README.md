# FaaS Runtime（本地函数即服务运行时）

基于 Spring Boot 的本地 FaaS 运行时，支持函数的发布、多版本共存、受约束调用、
**多实例实例级状态隔离**、**故障实例淘汰与替换**、按版本可配置的暖实例/并发容量、
实例自动伸缩与幂等调用，并在并发与异常场景下给出一致且可解释的结果分类。

## 架构

```
FaasRuntime（门面）
 ├── FunctionRegistry   函数多版本注册表（发布、版本解析；版本定义不可变）
 ├── FunctionInvoker    调用执行器（版本解析、超时、容量/并发、异常归类、
 │                       故障实例淘汰、幂等判定）
 │    └── IdempotencyStore  幂等键 -> 首次调用结果
 └── InstanceManager    (函数,版本) 暖实例池：多实例 + 实例内并发槽、
                         冷启动标记、故障淘汰、空闲缩容
```

REST 入口 `FaasController` 仅用于本地验证，核心逻辑均可脱离 HTTP 直接单测。

## 实例、状态与隔离规则

- 实例按 `(函数, 版本)` 成池管理。同版本可同时保有多个**互相独立**的实例，
  每个实例有稳定的 `instanceId`、独立的实例内并发信号量和独立的执行上下文。
- 发布有状态函数时提供 `FunctionHandlerFactory`：**每次冷启动一个新实例就调用
  一次 `create()`**，产出的处理器仅属于该实例。处理器内部字段即实例级状态：
  新实例看不到其它实例留下的内容，实例之间天然隔离。
- 调用结果与指标中可识别处理者：
  - `coldStart=true` 表示本次冷启动了新实例，`false` 表示复用暖实例；
  - `instanceId` 表示实际执行本次调用的实例（执行前被拒绝时为 `null`）。
- 新版本发布写入的是**不可变的新版本定义**（处理器 + 容量），不触碰老版本条目；
  老版本已在运行的实例继续按发布时的行为服务，发布 v2 不会淘汰/替换 v1 的实例。

## 故障实例的淘汰与替换语义

- 一次调用以 `TIMEOUT`（超时中断）或 `HANDLER_ERROR`（执行体抛异常）结束时，
  承载它的实例在释放的同一时刻被**立即从健康集合淘汰**，之后的调用永远不会
  再选中它；后续调用要么复用其它健康暖实例，要么冷启动一个新实例。
- 淘汰在实例池锁内与“获取实例”原子完成：一个调用要么拿到某一个实例、要么被
  明确拒绝，**同一个调用不可能被两个实例同时执行**。
- 被淘汰时若实例上仍有在飞调用，实例进入 `draining` 计数，等最后一个调用结束
  后自然回收；`draining` 只可能由“正在执行的调用”撑出，因此反复出问题也不会
  让实例总数无限上涨。健康实例数始终不超过 `maxWarmInstances`。
- 指标中：`evictedInstances` 为累计淘汰实例数，`drainingInstances` 为仍有
  在飞调用的已淘汰实例数。

## 容量上限与达到上限后的行为

容量**按函数版本**配置（发布时给出）：

| 配置 | 含义 | 默认（显式配置容量但省略该项时） |
|---|---|---|
| `maxWarmInstances` | 该版本最多保留/同时运行多少个健康实例 | 4 |
| `maxConcurrencyPerInstance` | 单个实例允许同时执行多少个调用 | 1 |

- 获取顺序（在池锁内原子判定，无中间态）：
  1. 优先复用“有空闲并发槽”的健康暖实例；
  2. 健康实例都忙、且健康实例数 < `maxWarmInstances` 时，**冷启动**新实例；
  3. 否则立即拒绝，状态为 `INSTANCE_CAPACITY_EXCEEDED`，**立即返回不排队**、
     不产生副作用、`instanceId=null`、`success()=false`。
- **未显式配置容量的老函数保持旧行为不变**：单实例 + 函数级并发信号量
  （默认 16），超限为 `CONCURRENCY_LIMIT_EXCEEDED`。两种拒绝状态可区分。
- 容量配置必须为正整数，否则发布返回 `INVALID_ARGUMENT`。

## 幂等与并发/容量交叉时的结果判定

`InvocationResult` 用三个字段把“成功 / 回放 / 拒绝”彻底分清：

- `status`：本次对外呈现的分类；
- `originStatus`：幂等回放时首次调用的原始状态（非回放时与 `status` 相同）；
- `replayed`：是否为幂等回放（未真正执行）。

判定规则：

| 场景 | `status` | `replayed` | `success()` |
|---|---|---|---|
| 真正执行成功 | `SUCCESS` | false | true |
| 命中幂等键，首次结果为成功 | `IDEMPOTENT_REPLAY` | true | true |
| 命中幂等键，但首次结果为超时/执行异常 | 首次原始状态（`TIMEOUT`/`HANDLER_ERROR`） | true | false |
| 因容量/并发到顶被拒（无论是否带幂等键） | `INSTANCE_CAPACITY_EXCEEDED` / `CONCURRENCY_LIMIT_EXCEEDED` | false* | **false** |
| 不同幂等键在容量到顶时 | 同上明确拒绝，绝不命中他人回放 | false | false |

\* 同一幂等键并发、且首个调用在“执行前”因容量被拒时：拒绝结果会告知同键等待者
（等待者拿到的是拒绝状态、`replayed=true`、`originStatus=拒绝状态`、仍不算成功），
随后该幂等键被**驱逐**——拒绝不永久占坑，容量释放后同键调用可重新真正执行。

因此：

- 同一个幂等键并发进来，**只有一个调用真正执行**，其余拿到同一份成功回放；
  同键等待者阻塞在幂等登记上，不参与容量竞争，不会被误拒。
- 被容量/并发拒绝的结果永远是“清楚的被拒结果”，**不会被当成某次成功结果的重放**，
  也不会在成功判断里被算成成功；真正命中成功回放的结果仍通过
  `IDEMPOTENT_REPLAY` + `replayed=true` 保留可识别性。

## 其它结果分类

| 状态 | 含义 |
|---|---|
| `SUCCESS` | 调用成功 |
| `IDEMPOTENT_REPLAY` | 命中幂等键且首次结果成功，回放首次结果，未重复执行 |
| `TIMEOUT` | 单次执行超过超时时间（默认 5000ms，可配置），实例同时被淘汰 |
| `HANDLER_ERROR` | 函数执行体抛异常（含实例初始化失败），错误信息保留原始异常类型，实例同时被淘汰 |
| `INSTANCE_CAPACITY_EXCEEDED` | 版本暖实例数或单实例并发到顶（按版本配置容量），立即拒绝不排队 |
| `CONCURRENCY_LIMIT_EXCEEDED` | 未配置容量的老函数超出函数级并发上限（默认 16），立即拒绝不排队 |
| `INVALID_ARGUMENT` | 参数非法（函数名为空、版本号/容量配置非法等） |
| `FUNCTION_NOT_FOUND` / `VERSION_NOT_FOUND` | 函数 / 版本不存在 |

## 空闲缩容

- 健康实例空闲超过 `idleTimeoutMillis`（默认 30s）且无在飞调用时，由后台线程
  回收（每个版本池可缩容到零）；缩容后首个调用触发冷启动。
- 缩容回收与获取实例在同一池锁内完成：竞争时调用要么复用旧实例、要么触发一次
  新冷启动，不丢失、不重复执行。

## 本地验证

### 运行单元测试

```bash
mvn test
```

- `FaasRuntimeTests`（原有 4 类，无回归）：并发发布、超时与结果分类、幂等重放、缩容竞争。
- `WarmInstanceTests`（本轮新增 10 个），日志打印 `实例=<id>` 与
  `判定路径=冷启动/复用暖实例/容量到顶/幂等回放/故障淘汰`：
  1. **实例状态隔离**：2 实例各自计数，冷/暖标记与 `instanceId` 正确，计数独立连续；
  2. **故障淘汰（异常）**：连续失败的实例 ID 两两不同，健康实例数始终不超上限，
     `evictedInstances` 递增，替换调用为冷启动；
  3. **故障淘汰（超时）**：超时实例立即退出健康池，新冷启动实例成功并被暖复用；
  4. **容量到顶立即拒绝**：1 实例/1 并发下第二个调用毫秒级返回
     `INSTANCE_CAPACITY_EXCEEDED`，不排队；
  5. **同键并发 × 容量**：8 个同键并发只真正执行 1 次，其余为成功回放，容量拒绝计数为 0；
  6. **同键领导者被容量拒绝**：拒绝不算成功、同键等待者看到拒绝状态，键被驱逐后可重试成功；
  7. **不同键 × 容量**：不同键打满容量时全部明确拒绝，不会被当成回放；
  8. **老函数不回归**：无容量配置的函数仍走函数级并发，超限为
     `CONCURRENCY_LIMIT_EXCEEDED`，拒绝不占幂等坑；
  9. **淘汰替换不丢不重**：200 次高并发、1/3 概率失败，断言
     “进入处理器次数 + 容量拒绝次数 == 调用总数”，且每次执行有且仅有一个归宿，
     draining 最终归零、健康实例数不超上限；
  10. **新版本不影响老实例**：发布 v2 后显式调 v1 仍走旧实例、保留旧状态，
      版本池按 `fn:version` 独立计数。

### 通过 REST 手工验证

```bash
mvn spring-boot:run
```

```bash
# 发布有状态计数函数：2 个暖实例、单实例并发 1（counter 为实例级状态工厂）
curl -X POST localhost:8080/faas/functions/counter/versions/1 \
     -H 'Content-Type: application/json' \
     -d '{"handlerType":"counter","maxWarmInstances":2,"maxConcurrencyPerInstance":1}'

# 连续调用：首次 coldStart=true，其后复用同一实例、计数在该实例上累加
curl -X POST localhost:8080/faas/functions/counter/invoke \
     -H 'Content-Type: application/json' -d '"a"'
curl -X POST localhost:8080/faas/functions/counter/invoke \
     -H 'Content-Type: application/json' -d '"b"'

# 不传容量参数 = 老行为（单实例 + 函数级并发 16）
curl -X POST localhost:8080/faas/functions/echo/versions/1 \
     -H 'Content-Type: application/json' -d '{"handlerType":"echo"}'

# 幂等：第二次 IDEMPOTENT_REPLAY 且 replayed=true
curl -X POST 'localhost:8080/faas/functions/echo/invoke?idempotencyKey=k1' \
     -H 'Content-Type: application/json' -d '"hello"'
curl -X POST 'localhost:8080/faas/functions/echo/invoke?idempotencyKey=k1' \
     -H 'Content-Type: application/json' -d '"hello"'

# 故障淘汰：failing 每次失败都换新实例（对比两次 instanceId 不同）
curl -X POST localhost:8080/faas/functions/flaky/versions/1 \
     -H 'Content-Type: application/json' \
     -d '{"handlerType":"failing","maxWarmInstances":1,"maxConcurrencyPerInstance":1}'
curl -X POST localhost:8080/faas/functions/flaky/invoke \
     -H 'Content-Type: application/json' -d '"x"'
curl -X POST localhost:8080/faas/functions/flaky/invoke \
     -H 'Content-Type: application/json' -d '"x"'

# 容量到顶立即拒绝：先占住唯一实例，第二个调用立即 INSTANCE_CAPACITY_EXCEEDED
curl -X POST localhost:8080/faas/functions/slow/versions/1 \
     -H 'Content-Type: application/json' \
     -d '{"handlerType":"sleep","sleepMillis":10000,"maxWarmInstances":1,"maxConcurrencyPerInstance":1}'
curl -X POST localhost:8080/faas/functions/slow/invoke \
     -H 'Content-Type: application/json' -d '"x"' &   # 占住
sleep 2
curl -X POST localhost:8080/faas/functions/slow/invoke \
     -H 'Content-Type: application/json' -d '"y"'    # 立即被容量拒绝

# 指标：健康/draining 实例数、冷启动、暖命中、故障淘汰、容量拒绝、按版本实例明细
curl localhost:8080/faas/metrics
```
