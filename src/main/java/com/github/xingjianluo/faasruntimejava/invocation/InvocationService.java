package com.github.xingjianluo.faasruntimejava.invocation;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;
import com.github.xingjianluo.faasruntimejava.idempotency.IdempotencyStore;
import com.github.xingjianluo.faasruntimejava.instance.InstancePool;
import com.github.xingjianluo.faasruntimejava.metrics.MetricsCollector;
import com.github.xingjianluo.faasruntimejava.model.CallResult;
import com.github.xingjianluo.faasruntimejava.model.FunctionDefinition;
import com.github.xingjianluo.faasruntimejava.model.InvocationRequest;
import com.github.xingjianluo.faasruntimejava.model.InvocationStatus;
import com.github.xingjianluo.faasruntimejava.registry.FunctionRegistry;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 调用编排：参数校验 -> 版本解析 -> 幂等 begin/join -> 并发配额 -> 限时执行 -> 结果分类。
 *
 * <p>结果分类一一对应失败原因，禁止统一错误码掩盖：
 * SUCCESS / REPLAY / TIMEOUT / FUNCTION_ERROR / CONCURRENCY_REJECTED /
 * INVALID_ARGUMENT / VERSION_NOT_FOUND。
 */
public final class InvocationService {

    private static final Logger log = LoggerFactory.getLogger(InvocationService.class);

    private final FunctionRegistry registry;
    private final IdempotencyStore idempotencyStore;
    private final MetricsCollector metrics;
    private final AtomicLong invocationSeq = new AtomicLong();
    private final ExecutorService executor;

    public InvocationService(FunctionRegistry registry, IdempotencyStore idempotencyStore,
                             MetricsCollector metrics) {
        this(registry, idempotencyStore, metrics, defaultExecutor());
    }

    private static ExecutorService defaultExecutor() {
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "faas-invocation-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        };
        return Executors.newCachedThreadPool(tf);
    }

    InvocationService(FunctionRegistry registry, IdempotencyStore idempotencyStore,
                      MetricsCollector metrics, ExecutorService executor) {
        this.registry = registry;
        this.idempotencyStore = idempotencyStore;
        this.metrics = metrics;
        this.executor = executor;
    }

    public CallResult invoke(InvocationRequest request) {
        String invocationId = "inv-" + invocationSeq.incrementAndGet();

        // 1. 参数非法：直接拒绝，不进入版本解析/幂等/执行，无任何副作用。
        if (request == null || request.functionName() == null
                || request.functionName().isBlank()) {
            log.warn("[{}] REJECTED reason=INVALID_ARGUMENT detail=blank function name", invocationId);
            return invalid(invocationId, request == null ? null : request.functionName(),
                    null, "function name must not be blank");
        }
        if (request.version() != null && request.version() < 1) {
            log.warn("[{}] REJECTED reason=INVALID_ARGUMENT detail=version={}",
                    invocationId, request.version());
            return invalid(invocationId, request.functionName(), request.version(),
                    "version must be a positive integer when provided");
        }

        // 2. 版本解析（含“函数不存在 / 版本不存在”的可区分失败）。
        FunctionRegistry.ResolvedVersion resolved;
        try {
            resolved = registry.resolve(request.functionName(), request.version());
        } catch (FaasException.FunctionNotFoundException | FaasException.VersionNotFoundException e) {
            String code = e instanceof FaasException.FunctionNotFoundException
                    ? "FUNCTION_NOT_FOUND" : "VERSION_NOT_FOUND";
            log.warn("[{}] REJECTED reason={} detail={}", invocationId, code, e.getMessage());
            CallResult r = new CallResult(InvocationStatus.VERSION_NOT_FOUND, request.functionName(),
                    request.version(), null, code, e.getMessage(), false, false, null,
                    invocationId, Instant.now());
            metrics.recordOutcome(request.functionName(),
                    request.version() == null ? -1 : request.version(),
                    InvocationStatus.VERSION_NOT_FOUND, false);
            return r;
        }
        FunctionDefinition def = resolved.definition();
        InstancePool pool = resolved.pool();
        String target = def.qualifiedName();

        // 3. 无幂等键：直接走单次执行。
        if (request.idempotencyKey() == null) {
            return executeOnce(invocationId, def, pool, request, null);
        }

        // 4. 幂等：先占位，再抢配额；重复调用等待重放。
        while (true) {
            IdempotencyStore.Join join = idempotencyStore.begin(
                    def.name(), def.version(), request.idempotencyKey());
            if (join instanceof IdempotencyStore.Leader leader) {
                CallResult result = executeOnce(invocationId, def, pool, request, leader);
                return result;
            }
            IdempotencyStore.Follower follower = (IdempotencyStore.Follower) join;
            try {
                Optional<CallResult> first = follower.awaitResult();
                if (first.isPresent()) {
                    CallResult r = asReplay(invocationId, first.get());
                    metrics.recordOutcome(def.name(), def.version(), InvocationStatus.REPLAY,
                            r.coldStart());
                    log.info("[{}] {} status=REPLAY original={} reason=idempotency-key:{}",
                            invocationId, target, r.invocationId(), request.idempotencyKey());
                    return r;
                }
                // Leader 未执行（被并发拒绝）并已回滚占位，循环重新争取执行权。
                log.info("[{}] {} idempotency leader was rejected, retrying as leader key={}",
                        invocationId, target, request.idempotencyKey());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return errorResult(def, invocationId, InvocationStatus.INVALID_ARGUMENT,
                        "INTERRUPTED", "interrupted while awaiting idempotent result", false, null);
            }
        }
    }

    /**
     * 抢一个实例并执行一次。leader 非 null 时负责幂等占位的最终处置。
     */
    private CallResult executeOnce(String invocationId, FunctionDefinition def,
                                   InstancePool pool, InvocationRequest request,
                                   IdempotencyStore.Leader leader) {
        String target = def.qualifiedName();

        // 5. 并发上限：非阻塞获取实例槽位。
        Optional<InstancePool.Lease> leaseOpt = pool.tryAcquire();
        if (leaseOpt.isEmpty()) {
            if (leader != null) {
                leader.fail(); // 被拒绝调用必须回滚幂等占位，不留副作用
            }
            metrics.recordOutcome(def.name(), def.version(),
                    InvocationStatus.CONCURRENCY_REJECTED, false);
            log.warn("[{}] {} REJECTED reason=CONCURRENCY_REJECTED limit={}",
                    invocationId, target, pool.maxInstances());
            return new CallResult(InvocationStatus.CONCURRENCY_REJECTED, def.name(),
                    def.version(), null, "CONCURRENCY_LIMIT_EXCEEDED",
                    "function concurrency limit " + pool.maxInstances() + " reached",
                    false, false, null, invocationId, Instant.now());
        }

        InstancePool.Lease lease = leaseOpt.get();
        final InstancePool.Lease acquired = lease;
        boolean coldStart = lease.coldStart();
        String instanceId = lease.instance().id();
        long timeoutMs = def.config().timeout().toMillis();

        try {
            // 6. 限时执行：超时与业务异常分离。
            String output;
            try {
                output = executor.submit(
                        () -> acquired.instance().handler().handle(request.payload())
                ).get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                lease.discard(); // 超时线程可能仍在运行，实例不可复用
                lease = null;
                CallResult r = new CallResult(InvocationStatus.TIMEOUT, def.name(), def.version(),
                        null, "TIMEOUT", "execution exceeded timeout of " + timeoutMs + "ms",
                        coldStart, false, instanceId, invocationId, Instant.now());
                metrics.recordOutcome(def.name(), def.version(), InvocationStatus.TIMEOUT,
                        coldStart);
                if (leader != null) {
                    leader.complete(r);
                }
                log.warn("[{}] {} status=TIMEOUT timeoutMs={} instance={} reason=deadline-exceeded",
                        invocationId, target, timeoutMs, instanceId);
                return r;
            } catch (java.util.concurrent.ExecutionException ee) {
                Throwable cause = ee.getCause() == null ? ee : ee.getCause();
                lease.release();
                CallResult r = new CallResult(InvocationStatus.FUNCTION_ERROR, def.name(),
                        def.version(), null, "FUNCTION_ERROR",
                        cause.getClass().getSimpleName() + ": " + cause.getMessage(),
                        coldStart, false, instanceId, invocationId, Instant.now());
                metrics.recordOutcome(def.name(), def.version(), InvocationStatus.FUNCTION_ERROR,
                        coldStart);
                if (leader != null) {
                    leader.complete(r);
                }
                log.warn("[{}] {} status=FUNCTION_ERROR instance={} reason={}:{}",
                        invocationId, target, instanceId,
                        cause.getClass().getSimpleName(), cause.getMessage());
                return r;
            }

            lease.release();
            CallResult r = new CallResult(InvocationStatus.SUCCESS, def.name(), def.version(),
                    output, null, null, coldStart, false, instanceId, invocationId,
                    Instant.now());
            metrics.recordOutcome(def.name(), def.version(), InvocationStatus.SUCCESS,
                    coldStart);
            if (leader != null) {
                leader.complete(r);
            }
            log.info("[{}] {} status=SUCCESS coldStart={} instance={} reason=normal-completion",
                    invocationId, target, coldStart, instanceId);
            return r;

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (lease != null) {
                lease.discard();
            }
            CallResult r = errorResult(def, invocationId, InvocationStatus.TIMEOUT,
                    "INTERRUPTED", "invocation interrupted", coldStart, instanceId);
            metrics.recordOutcome(def.name(), def.version(), InvocationStatus.TIMEOUT,
                    coldStart);
            if (leader != null) {
                leader.complete(r);
            }
            return r;
        } catch (RuntimeException unexpected) {
            if (lease != null) {
                lease.discard();
            }
            if (leader != null) {
                leader.fail();
            }
            throw unexpected;
        }
    }

    private CallResult invalid(String invocationId, String name, Integer version, String msg) {
        return new CallResult(InvocationStatus.INVALID_ARGUMENT, name, version, null,
                "INVALID_ARGUMENT", msg, false, false, null, invocationId, Instant.now());
    }

    private CallResult errorResult(FunctionDefinition def, String invocationId,
                                   InvocationStatus status, String code, String msg,
                                   boolean coldStart, String instanceId) {
        return new CallResult(status, def.name(), def.version(), null, code, msg,
                coldStart, false, instanceId, invocationId, Instant.now());
    }

    private static CallResult asReplay(String replayInvocationId, CallResult first) {
        // 重放结果沿用首次执行的 invocationId（作为去重溯源标识）；replayInvocationId 仅记录在说明中
        return new CallResult(InvocationStatus.REPLAY, first.functionName(), first.version(),
                first.body(), "REPLAY",
                "replayed idempotent result of invocation " + first.invocationId()
                        + " (replayed by " + replayInvocationId + ")",
                first.coldStart(), true, first.instanceId(), first.invocationId(), Instant.now());
    }
}
