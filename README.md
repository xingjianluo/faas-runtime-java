# FaaS Runtime（本地函数即服务运行时）

基于 Spring Boot 的本地 FaaS 运行时，支持函数的发布、多版本共存、受约束调用、
实例自动伸缩与幂等调用，并在并发与异常场景下给出一致且可解释的结果分类。

## 架构

```
FaasRuntime（门面）
 ├── FunctionRegistry   函数多版本注册表（发布、版本解析）
 ├── FunctionInvoker    调用执行器（超时、并发上限、异常归类、幂等）
 │    └── IdempotencyStore  幂等键 -> 首次调用结果
 └── InstanceManager    暖实例管理与空闲缩容（冷启动标记）
```

REST 入口 `FaasController` 仅用于本地验证，核心逻辑均可脱离 HTTP 直接单测。

## 版本兼容规则

- 同一函数可共存多个版本，**历史版本不可覆盖**：重复发布同一 `(函数, 版本)`
  返回 `DUPLICATE_VERSION`。
- 同一函数的发布串行化：并发发布抢不到发布锁时立即以
  `CONCURRENT_MODIFICATION` 拒绝（可安全重试），不允许状态竞争产生半成品版本。
- 版本号与处理器通过单次原子写入同时对外可见，**不存在“版本已可见却不可用”
  的中间状态**。
- 调用时 `version <= 0` 表示使用当前生效版本（已发布的最大版本号）；
  显式指定历史版本时，行为与发布时完全一致（处理器条目一经写入不可变）。
- 版本不存在返回 `VERSION_NOT_FOUND`，函数不存在返回 `FUNCTION_NOT_FOUND`，
  两者可区分。

## 并发与超时语义

每次调用的结果被归类为 `InvocationStatus` 之一，**不以统一错误掩盖**：

| 状态 | 含义 |
|---|---|
| `SUCCESS` | 调用成功 |
| `IDEMPOTENT_REPLAY` | 命中幂等键，回放首次结果，未重复执行 |
| `TIMEOUT` | 单次执行超过超时时间（默认 5000ms，可配置） |
| `HANDLER_ERROR` | 函数执行体抛出异常，错误信息保留原始异常类型 |
| `CONCURRENCY_LIMIT_EXCEEDED` | 超出函数级并发上限（默认 16，可配置），立即拒绝不排队 |
| `INVALID_ARGUMENT` | 参数非法（函数名为空等） |
| `FUNCTION_NOT_FOUND` / `VERSION_NOT_FOUND` | 函数 / 版本不存在 |

- 超时通过中断取消执行（虚拟线程承载），超时调用会在 `finally` 中释放
  并发槽位与实例占用，**不污染后续调用**。
- 被拒绝或失败的调用不产生副作用；参数与版本校验在幂等登记之前完成，
  非法调用不占用幂等键。

## 伸缩与幂等约定

- 实例按 `(函数, 版本)` 维护；空闲超过 `idleTimeoutMillis`（默认 30s）且
  无进行中调用时，由后台线程缩容到零。
- 缩容后首个调用触发**冷启动**，结果中 `coldStart=true`；复用暖实例则为
  `false`，指标接口同时统计冷启动次数与暖实例命中次数。
- 获取实例与缩容回收在同一 key 上原子完成：缩容与并发调用同时发生时，
  调用要么复用旧实例、要么触发一次新冷启动，**不丢失、不重复执行**；
  有进行中调用的实例不会被回收。
- 幂等：传入相同 `idempotencyKey` 的调用只执行一次副作用。
  执行期间到达的同键调用阻塞等待首个结果；执行完成后的同键调用立即回放，
  状态为 `IDEMPOTENT_REPLAY`，返回值与首次一致。
- 并发超限等“执行前拒绝”的结果不会永久占用幂等键：拒绝结果告知等待者后
  键被驱逐，后续同键调用可重新尝试。

## 本地验证

### 运行单元测试

```bash
mvn test
```

`FaasRuntimeTests` 覆盖四类场景，日志中打印调用/版本标识与判定依据：

1. **并发发布**：16 线程并发发布同一版本，断言恰好 1 个成功，其余为
   `DUPLICATE_VERSION` / `CONCURRENT_MODIFICATION`，且版本可见即可用；
2. **调用超时**：断言 `TIMEOUT` / `HANDLER_ERROR` / `CONCURRENCY_LIMIT_EXCEEDED`
   / `INVALID_ARGUMENT` / `FUNCTION_NOT_FOUND` 五类状态可区分且互不污染；
3. **幂等重放**：串行与并发同键调用，断言副作用只执行一次、回放结果一致；
4. **缩容竞争**：缩容持续发生的同时发起 40 次错落调用，断言执行次数与
   调用次数严格相等（不丢失、不重复），且期间确实发生冷启动。

### 通过 REST 手工验证

```bash
mvn spring-boot:run
```

```bash
# 发布 echo 函数 v1（内置 handler 类型：echo / uppercase / sleep / failing）
curl -X POST localhost:8080/faas/functions/hello/versions/1 \
     -H 'Content-Type: application/json' -d '{"handlerType":"echo"}'

# 调用当前生效版本
curl -X POST 'localhost:8080/faas/functions/hello/invoke' \
     -H 'Content-Type: application/json' -d '"world"'

# 幂等调用（第二次返回 IDEMPOTENT_REPLAY）
curl -X POST 'localhost:8080/faas/functions/hello/invoke?idempotencyKey=k1' \
     -H 'Content-Type: application/json' -d '"world"'

# 发布 sleep 函数验证超时（默认超时 5s，sleep 10s 将返回 TIMEOUT）
curl -X POST localhost:8080/faas/functions/slow/versions/1 \
     -H 'Content-Type: application/json' -d '{"handlerType":"sleep","sleepMillis":10000}'
curl -X POST 'localhost:8080/faas/functions/slow/invoke' \
     -H 'Content-Type: application/json' -d '"x"'

# 查看指标（活跃实例数 / 冷启动次数 / 暖实例命中次数）
curl localhost:8080/faas/metrics
```
