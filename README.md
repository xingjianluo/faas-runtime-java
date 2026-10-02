# FaaS Runtime（本地函数即服务运行时）

基于 Spring Boot 的本地 FaaS 运行时，支持函数的发布、多版本共存、受约束调用、
**多暖实例与实例级私有状态**、**故障实例淘汰替换**、**按版本配置容量**、
实例自动伸缩与幂等调用，并在并发与异常场景下给出一致且可解释的结果分类。

## 架构

```
FaasRuntime（门面）
 ├── FunctionRegistry   函数多版本注册表（发布、版本解析、版本容量配置）
 ├── FunctionInvoker    调用执行器（超时、并发/容量约束、异常归类、
 │                        实例上下文装配、故障淘汰、幂等）
 │    └── IdempotencyStore  幂等键 -> 首次调用结果
 └── InstanceManager    按 (函数, 版本) 的暖实例池：多实例并存、
                         私有状态、故障淘汰、空闲缩容（冷启动标记）
```

REST 入口 `FaasController` 仅用于本地验证，核心逻辑均可脱离 HTTP 直接单测。

## 版本兼容规则

- 同一函数可共存多个版本，**历史版本不可覆盖**：重复发布同一 `(函数, 版本)`
  返回 `DUPLICATE_VERSION`。
- 同一函数的发布串行化：并发发布抢不到发布锁时立即以
  `CONCURRENT_MODIFICATION` 拒绝（可安全重试），不允许状态竞争产生半成品版本。
- 版本号、处理器、容量配置通过单次原子写入同时对外可见，
  **不存在“版本已可见却不可用”的中间状态**。
- 调用时 `version <= 0` 表示使用当前生效版本（已发布的最大版本号）；
  显式指定历史版本时，行为与发布时完全一致（处理器条目一经写入不可变）。
- 实例池按版本隔离：**发布新版本不会触碰老版本已在运行的实例**，老版本继续
  复用自己的暖实例、延续其内部状态提供服务；新版本冷启动属于自己的实例。
- 版本不存在返回 `VERSION_NOT_FOUND`，函数不存在返回 `FUNCTION_NOT_FOUND`。

## 实例状态与隔离规则

- 发布时可提供 `StatefulFunctionHandler`（继承 `FunctionHandler`）：
  - `initInstanceState()`：**每个实例冷启动时恰好调用一次**，创建该实例的
    私有状态；
  - `handle(input, InstanceContext)`：每次调用拿到该实例的 `InstanceContext`，
    其中含 `instanceId` 与该实例独占的 `state`。
- 同一个 (函数, 版本) 下可同时存在多个实例，**实例之间状态严格隔离**：
  新实例从空状态起步，看不到任何其他实例（包括同版本兄弟实例和其他版本）
  留下的数据。普通 `FunctionHandler` 没有实例状态，行为不变。
- 每次调用结果都带 `instanceId` 与 `coldStart`：
  - `coldStart=true` 表示本次冷启动了一个新实例；`false` 表示复用暖实例；
  - `instanceId` 形如 `fn:1#inst-7`，日志中会打印“冷启动新实例 / 复用暖实例”。
- 暖实例在无在飞调用且空闲超过 `idleTimeoutMillis`（默认 30s）后被回收；
  有在飞调用的实例不会被回收。

## 故障实例的淘汰与替换语义

- 只要某次调用在某实例上发生 **超时（`TIMEOUT`）或执行体异常
  （`HANDLER_ERROR`）**，该实例在调用结束的释放动作中被**原子摘除淘汰**：
  - 摘除在版本池的同一临界区内完成，之后的新调用绝不会再被分配给它；
  - 后续调用要么复用池中其他**健康**暖实例，要么在容量内**冷启动新实例**。
- **不丢调用、不重复执行**：
  - 每个调用只被分配给唯一一个实例，不存在同一调用被两个实例同时执行；
  - 实例摘除与新调用获取在同一把池锁上串行，调用要么落到健康实例、
    要么冷启动、要么拿到明确的“容量拒绝”，不会悬空也不会重试导致重复副作用。
- **实例数有硬上限**：实例数永远不超过该版本配置的 `maxWarmInstances`，
  反复故障淘汰/替换也不会让实例数无限上涨。淘汰次数可通过指标 `evictions`
  查看，日志中以 `淘汰故障实例` 标记。
- 成功返回的实例保持健康继续服务；被中断（`InterruptedException`）同样淘汰。

## 容量上限与达到上限后的行为

容量按**函数版本**在发布时配置（均为正整数，否则发布返回 `INVALID_ARGUMENT`）：

| 配置 | 含义 |
|---|---|
| `maxWarmInstances` | 该版本最多保留/创建的暖实例数 |
| `maxConcurrencyPerInstance` | 单个实例允许同时执行的调用数 |

- 分配规则（在池锁内一步完成）：先找一个**健康且有空闲槽位**的暖实例复用；
  没有可复用实例且实例数未到上限时，冷启动一个新实例；
  **所有实例都满载且实例数已达上限时立即拒绝，不排队**。
- 被拒返回 `INSTANCE_CAPACITY_EXCEEDED`，`instanceId/value` 均为空，
  **不算成功**（`success()=false`），错误信息中包含池大小与每实例并发上限。
- **未显式配置容量的老函数保持原行为**：每版本一个暖实例，受函数级全局
  并发信号量（默认 16，可用 `setMaxConcurrencyPerFunction` 调）约束，
  到顶返回 `CONCURRENCY_LIMIT_EXCEEDED`。

## 幂等与并发交叉时的判定依据

- 同一 `idempotencyKey` 并发进入时，只有一个调用真正执行函数（副作用一次）；
  其余调用等待首个结果。
- **真正命中回放**：包装为 `IDEMPOTENT_REPLAY`，返回值、错误、`coldStart`、
  `instanceId` 与首次一致，保留可识别性，且按成功计（`success()=true`）。
- **首个调用因容量/并发被拒**：
  - 等待者拿到的是**原始被拒状态**（`INSTANCE_CAPACITY_EXCEEDED` 或
    `CONCURRENCY_LIMIT_EXCEEDED`），**不包装成回放、`success()=false`**；
  - 该幂等键同时被**驱逐**，容量释放后同键调用可以真正执行一次，
    而不是永久回放“被拒绝”；
  - 被拒调用不执行函数体、不占用实例槽位。
- `InvocationResult.success()` 只对 `SUCCESS` 与 `IDEMPOTENT_REPLAY` 为真，
  因此“是否成功”的判断天然把容量/并发拒绝排除在外。
- 参数与版本校验在幂等登记之前完成，非法调用不占用幂等键。

### 调用结果分类一览

| 状态 | 含义 | success() |
|---|---|---|
| `SUCCESS` | 调用成功 | true |
| `IDEMPOTENT_REPLAY` | 命中幂等键，回放首次结果，未重复执行 | true |
| `TIMEOUT` | 单次执行超时，实例被淘汰 | false |
| `HANDLER_ERROR` | 执行体抛异常（或实例初始化失败），实例被淘汰 | false |
| `INSTANCE_CAPACITY_EXCEEDED` | 暖实例池打满（显式容量版本），立即拒绝 | false |
| `CONCURRENCY_LIMIT_EXCEEDED` | 老函数函数级并发上限，立即拒绝 | false |
| `INVALID_ARGUMENT` / `FUNCTION_NOT_FOUND` / `VERSION_NOT_FOUND` | 参数/目标问题 | false |

## 本地验证

### 运行单元测试

```bash
mvn test
```

- `FaasRuntimeTests`（原有 4 个场景，持续保持绿色、无回归）：并发发布、
  调用超时分类、幂等重放、缩容竞争不丢不重。
- `WarmInstanceTests`（本轮新增 4 个场景）：
  1. **实例状态隔离**：3 实例并发冷启动后各自只看到自己的状态；第二轮精确
     复用原实例且状态延续；发布 v2 后老版本 3 个实例原样保留并继续服务；
  2. **故障实例淘汰**：异常/超时实例立即摘除，`evictions` 计数增长，后续
     调用冷启动全新实例，被淘汰 ID 不再出现；
  3. **容量×幂等交叉分类**：满容量下同键并发全部拿到原始
     `INSTANCE_CAPACITY_EXCEEDED`（非回放、非成功）、零执行；键被驱逐后
     容量释放可真正执行一次；空闲容量下同键并发恰好 1 次执行、其余为
     可识别的成功回放；老函数仍返回 `CONCURRENCY_LIMIT_EXCEEDED`；
  4. **淘汰替换不丢不重**：192 次错落并发（约 1/4 注入异常/超时），断言
     成功+异常+超时+拒绝的分类总数严格等于调用数，成功执行体次数等于
     成功调用数，实例数始终不超过上限，且存在暖复用与冷启动替换。

所有关键节点（冷启动、暖复用、容量拒绝、超时/异常淘汰、幂等回放/被拒原样
传播）都在日志中打印实例 ID 与“判定路径”。

### 通过 REST 手工验证

```bash
mvn spring-boot:run   # 若 8080 被占用：-Dspring-boot.run.arguments=--server.port=18099
```

```bash
# 发布有状态 counter，容量 2 实例 x 每实例并发 1（不传容量即为老行为）
curl -X POST localhost:8080/faas/functions/cnt/versions/1 \
     -H 'Content-Type: application/json' \
     -d '{"handlerType":"counter","maxWarmInstances":2,"maxConcurrencyPerInstance":1}'

# 连续调用：返回体带 instanceId、coldStart，value.state 是该实例的私有累积
curl -X POST localhost:8080/faas/functions/cnt/invoke \
     -H 'Content-Type: application/json' -d '"x1"'
curl -X POST localhost:8080/faas/functions/cnt/invoke \
     -H 'Content-Type: application/json' -d '"x2"'

# 指标：activeInstances / coldStarts / warmHits / evictions / activeInstanceDetails
curl localhost:8080/faas/metrics

# failing：HANDLER_ERROR 且该实例被淘汰（再看 metrics，evictions 增加）
curl -X POST localhost:8080/faas/functions/bad/versions/1 \
     -H 'Content-Type: application/json' \
     -d '{"handlerType":"failing","maxWarmInstances":1,"maxConcurrencyPerInstance":1}'
curl -X POST localhost:8080/faas/functions/bad/invoke \
     -H 'Content-Type: application/json' -d '"x"'

# 容量到顶：先发一个 sleep 占住唯一实例，第二个调用立即得到
# INSTANCE_CAPACITY_EXCEEDED（不排队）；同键并发时被拒不回放
curl -X POST localhost:8080/faas/functions/slow/versions/1 \
     -H 'Content-Type: application/json' \
     -d '{"handlerType":"sleep","sleepMillis":5000,"maxWarmInstances":1,"maxConcurrencyPerInstance":1}'
curl -X POST localhost:8080/faas/functions/slow/invoke \
     -H 'Content-Type: application/json' -d '"hold"' &
curl -X POST localhost:8080/faas/functions/slow/invoke \
     -H 'Content-Type: application/json' -d '"overflow"'
```

内置 handler 类型：`echo` / `uppercase` / `sleep` / `failing` / `counter`。
