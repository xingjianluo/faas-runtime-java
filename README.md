# 本地 FaaS 运行时（Java / Spring Boot）

一个完全在本地进程内运行的函数即服务运行时，支持**多版本发布、限时调用、并发约束、
空闲缩容与幂等重放**。所有状态保存在内存中，不依赖外部中间件，可直接通过
Java API 或 HTTP 接口使用。

## 1. 快速开始

```bash
# 构建并运行全部测试（17 个用例）
mvn test

# 启动服务（默认端口 8080）
mvn spring-boot:run
```

发布一个函数版本：

```bash
curl -s -X POST http://localhost:8080/faas/functions \
  -H 'Content-Type: application/json' \
  -d '{"name":"hello","version":1,"handlerType":"echo",
       "timeoutMs":2000,"concurrency":4,"idleTtlMs":30000}'
```

调用（可指定版本；省略 `version` 时使用当前生效版本）：

```bash
curl -s -X POST http://localhost:8080/faas/functions/hello/invocations \
  -H 'Content-Type: application/json' \
  -d '{"version":1,"payload":"hi","idempotencyKey":"order-123"}'
```

查看指标：

```bash
curl -s 'http://localhost:8080/faas/functions/hello/metrics?version=1'
```

内置处理器（`handlerType`）：

| 类型 | 行为 | 用途 |
| --- | --- | --- |
| `echo` | 原样返回 payload | 常规成功路径、版本冻结验证 |
| `sleep` | 按 payload 毫秒数睡眠后返回 | 超时测试 |
| `failing` | 每次抛 `IllegalStateException` | 函数执行异常测试 |
| `sideEffect` | 实例内自增计数并返回 `side-effect-N` | 复用 / 实例隔离测试 |
| `globalSideEffect` | 全局自增计数 `global-side-effect-N` | 缩容竞争下的副作用去重测试 |

## 2. 版本兼容规则

- 版本号为**正整数**，同一函数内必须**单调递增**：首个版本必须是 `v1`，
  之后只能发布 `vN+1`。越序发布被拒绝，错误为 `InvalidVersionException`（HTTP 400）。
- 历史版本**不可覆盖、不可变**：重复发布已存在版本被拒绝，错误为
  `VersionAlreadyExistsException`（HTTP 409），与越序版本是两种可区分的失败原因。
- 每次发布把 `handlerType`、超时、并发上限、空闲 TTL **固化**在不可变的
  `FunctionDefinition` 中。调用历史版本时始终使用它发布时的配置与代码，行为与发布时一致。
- 未指定版本的调用命中“当前生效版本”（最新发布成功的版本）；显式指定版本则精确命中。
- 并发发布按**每个函数一把发布锁**串行化，因此“版本竞争”有确定结果：恰好一个发布成功，
  其余得到 `VERSION_ALREADY_EXISTS`。
- **不存在“版本可见却不可用”的中间状态**：处理器的解析与可实例化校验发生在版本入表之前、
  同一把锁内；校验失败则本次发布不留任何状态（函数也不会被创建）。
- 调用一个从未发布的函数 → `FUNCTION_NOT_FOUND`；调用存在函数的缺失版本 →
  `VERSION_NOT_FOUND`，二者在 `errorCode` 上可区分。

## 3. 并发与超时语义

### 调用结果分类（绝不以统一错误码掩盖）

| `status` | `errorCode` | 含义 | 是否执行过函数 |
| --- | --- | --- | --- |
| `SUCCESS` | - | 正常完成 | 是 |
| `REPLAY` | `REPLAY` | 命中幂等重放 | 否（重放首次结果） |
| `TIMEOUT` | `TIMEOUT` | 超过单次调用超时 | 不确定（保守视为已执行） |
| `FUNCTION_ERROR` | `FUNCTION_ERROR` | 处理器抛异常 | 是 |
| `CONCURRENCY_REJECTED` | `CONCURRENCY_LIMIT_EXCEEDED` | 超出函数级并发上限 | 否 |
| `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | 函数名/版本号非法 | 否 |
| `VERSION_NOT_FOUND` | `FUNCTION_NOT_FOUND` / `VERSION_NOT_FOUND` | 目标不存在 | 否 |

HTTP 层：调用类失败统一返回 200，由响应体 `status`/`errorCode` 区分；
发布类失败返回可区分的 4xx（400/404/409）。

### 并发上限

- 每个函数版本维护一个独立实例池，**实例池容量 == 发布时配置的并发上限**。
- 获取实例是非阻塞的：有空闲实例则复用，否则在容量内冷启动新建，
  容量已满立即返回 `CONCURRENCY_REJECTED`（不会排队、不会占用资源）。
- 被拒绝的调用**没有任何副作用**：不计数业务副作用、不污染幂等键（占位会被回滚）。

### 超时

- 每次调用在独立工作线程中执行，调用方按版本配置的 `timeout` 限时等待。
- 超时后该实例被**丢弃（discard）**而不是归还空闲队列——因为工作线程可能仍在运行，
  复用它会污染后续调用；下一次调用会冷启动一个全新处理器实例。
- 业务异常与超时严格分离：处理器抛出的异常映射为 `FUNCTION_ERROR`，
  只有到达时限才映射为 `TIMEOUT`。

## 4. 伸缩与幂等约定

### 实例伸缩 / 冷启动

- 实例在首次需要时冷启动；正常归还后进入空闲队列供后续调用复用。
- 缩容器（默认每 `faas.reaper-interval-ms=5000ms` 一轮）回收空闲时长超过该版本
  `idleTtl` 的实例，可一直缩到 **0**。
- 实例获取与缩容摘除在**同一把池锁**内互斥，缩容只会摘除“确定空闲”的实例，
  **in-flight 实例绝不会被回收**。因此缩容与调用并发时，调用不会丢失、不会重复执行。
- 冷启动在调用结果（`coldStart` 字段）与指标（`coldStarts`）中均可观测；
  缩容到 0 后的下一次调用必然是冷启动。

### 调用幂等

- 幂等域为 **函数名 + 版本 + 幂等键**；不传 `idempotencyKey` 表示不去重。
- 编排顺序是“**先占幂等槽，再抢并发配额**”：同一幂等键的首个调用成为 leader，
  并发的重复调用成为 follower 并等待 leader 的结果，随后以 `REPLAY` 返回——
  重复调用不会各自抢占并发槽，也不会重复执行副作用。
- 只有“确定执行过”的终局会被固化重放：`SUCCESS`、`FUNCTION_ERROR`、`TIMEOUT`。
  其中超时因副作用是否发生不可知，**保守固化**以避免重复副作用。
- 若 leader 因超并发未能执行，其幂等占位会被**回滚**，该键之后可正常执行一次；
  参数非法、版本不存在等请求在进入幂等区之前就被拒绝，根本不会占用幂等键。
- 重放结果沿用首次执行的 `invocationId`（响应消息中附带重放请求自身的 id），
  便于链路溯源与状态复现。

## 5. 关键类一览

```
exception/FaasException        可区分的失败原因体系
model/                         FunctionDefinition / InvocationRequest / CallResult / InstanceMetrics
handler/                       FunctionHandler SPI、工厂与内置处理器
registry/FunctionRegistry      原子发布、版本树、当前版本指针
instance/InstancePool          冷启动/复用/并发槽/空闲缩容（池锁互斥）
idempotency/IdempotencyStore   leader/follower 幂等占位与重放
invocation/InvocationService   调用编排与结果分类
scaling/IdleReaper             定时空闲缩容（也可手动 reapOnce）
metrics/MetricsCollector       原子指标计数
runtime/FaasRuntime            对外门面（可 Spring 注入，也可直接 new）
web/FaasController             HTTP 入口与错误码映射
```

## 6. 本地验证方法

纯 Java 用法（无需启动 Web 服务）：

```java
FaasRuntime runtime = new FaasRuntime(
        new DefaultFunctionHandlerFactory(), new RuntimeProperties());

runtime.publish("hello", 1, "echo",
        Duration.ofSeconds(2), 4, Duration.ofSeconds(30));

CallResult r1 = runtime.invoke(
        new InvocationRequest("hello", 1, "ping", "order-123"));
CallResult r2 = runtime.invoke(
        new InvocationRequest("hello", 1, "ping", "order-123"));
// r1.status() == SUCCESS，r2.status() == REPLAY，副作用只执行一次

runtime.reapIdle(); // 手动触发缩容（测试可复现）
InstanceMetrics m = runtime.metrics("hello", 1);
```

四类并发/异常场景的测试位于
`src/test/java/com/github/xingjianluo/faasruntimejava/tests`：

| 测试类 | 覆盖场景 |
| --- | --- |
| `ConcurrentPublishTest` | 并发发布：同版本竞争、单调版本、历史版本可调用、非法处理器无残留 |
| `InvocationTimeoutTest` | 超时 / 函数异常 / 超并发 / 非法参数分类，冷启动与复用可观测 |
| `IdempotentReplayTest` | 串行与并发重放、拒绝不占幂等键、业务异常固化、版本级幂等域 |
| `ScaleContentionTest` | 空闲缩到 0、缩容与 40 并发调用竞争不丢失不重复、在飞实例不被回收 |
| `FaasHttpSmokeTest` | 经真实 HTTP 端口的发布/409/400/调用/重放/指标端到端验证 |

```bash
# 运行单类场景
mvn test -Dtest=ScaleContentionTest

# 运行时日志会打印调用/版本标识与判定依据，例如：
# PUBLISHED hello#v1 ... current=v1
# [inv-8] hello#v1 status=SUCCESS coldStart=true instance=hello#v1-inst-3 reason=normal-completion
# IDEMPOTENCY-REPLAY key=hello#v1:order-123 originalInvocationId=inv-8 -> replay
# [hello#v1] SCALE-IN evict instance=... remain=0
```

## 7. 范围说明

本运行时面向本地验证与语义澄清：状态为进程内内存态，重启即清空；
不包含持久化、分布式共识、跨进程配额与鉴权。上述一致性保证（发布原子性、
不重复副作用、缩容不丢调用）均限定在单进程内。
