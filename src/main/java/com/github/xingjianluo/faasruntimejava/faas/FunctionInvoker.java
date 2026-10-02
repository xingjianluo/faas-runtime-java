package com.github.xingjianluo.faasruntimejava.faas;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 调用执行器：负责版本解析、超时控制、并发/容量约束、异常归类、
 * 故障实例淘汰与幂等回放。
 *
 * <p>两类容量模式：
 * <ul>
 *   <li>遗留模式（未显式配置容量的老函数）：单实例 + 函数级并发信号量
 *       （默认 16），超限返回 {@link InvocationStatus#CONCURRENCY_LIMIT_EXCEEDED}，
 *       行为与旧版一致；</li>
 *   <li>按版本配置容量：多实例、每实例独立并发槽，由 {@link InstanceManager}
 *       在池锁内原子判定，到顶返回 {@link InvocationStatus#INSTANCE_CAPACITY_EXCEEDED}，
 *       立即拒绝不排队。</li>
 * </ul>
 *
 * <p>故障淘汰：TIMEOUT / HANDLER_ERROR 的调用在释放实例时将其标记为不健康，
 * 实例立即退出健康集合，后续调用只会落到其他健康实例或触发新冷启动。
 *
 * <p>幂等交叉：容量/并发拒绝的判定先于幂等结果的长期登记，拒绝结果告知同键
 * 等待者后驱逐幂等键，使其可重试；拒绝回放直接呈现拒绝状态（不算成功），
 * 成功回放则呈现 {@link InvocationStatus#IDEMPOTENT_REPLAY}。
 */
@Component
public class FunctionInvoker {

    private static final Logger log = LoggerFactory.getLogger(FunctionInvoker.class);

    private final FunctionRegistry registry;
    private final InstanceManager instanceManager;
    private final IdempotencyStore idempotencyStore;

    /** 遗留模式函数级并发槽位：函数名 -> 信号量（仅 legacy 函数使用） */
    private final ConcurrentMap<String, Semaphore> legacyConcurrencySlots = new ConcurrentHashMap<>();
    /** 执行函数的线程池：虚拟线程，超时通过中断取消 */
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** 单次调用超时，默认 5000ms，测试可调 */
    private volatile long invokeTimeoutMillis = 5_000;
    /** 遗留模式函数级并发上限，默认 16，测试可调 */
    private volatile int maxConcurrencyPerFunction = 16;

    public FunctionInvoker(FunctionRegistry registry, InstanceManager instanceManager,
                           IdempotencyStore idempotencyStore) {
        this.registry = registry;
        this.instanceManager = instanceManager;
        this.idempotencyStore = idempotencyStore;
    }

    /**
     * 同步调用函数。
     *
     * @param functionName   函数名
     * @param version        版本号，<= 0 表示当前生效版本
     * @param input          入参
     * @param idempotencyKey 幂等键，可为 null（不启用幂等）
     */
    public InvocationResult invoke(String functionName, int version, Object input, String idempotencyKey) {
        String invocationId = UUID.randomUUID().toString();
        long start = System.nanoTime();

        // 1. 参数与版本校验（在幂等登记之前，失败不占用幂等键）
        if (functionName == null || functionName.isBlank()) {
            return failure(invocationId, functionName, version, InvocationStatus.INVALID_ARGUMENT,
                    "functionName must not be blank", start, null);
        }
        if (!registry.containsFunction(functionName)) {
            return failure(invocationId, functionName, version, InvocationStatus.FUNCTION_NOT_FOUND,
                    "function '" + functionName + "' not found", start, null);
        }
        var resolvedVersion = registry.resolveVersion(functionName, version);
        if (resolvedVersion.isEmpty()) {
            return failure(invocationId, functionName, version, InvocationStatus.VERSION_NOT_FOUND,
                    "version " + version + " of function '" + functionName + "' not found", start, null);
        }
        int actualVersion = resolvedVersion.get();
        FunctionDefinition definition = registry.resolve(functionName, actualVersion).orElseThrow();

        // 2. 幂等登记：同键调用等待首个结果，不重复执行
        boolean idempotent = idempotencyKey != null && !idempotencyKey.isBlank();
        if (idempotent) {
            var existing = idempotencyStore.beginOrAwait(idempotencyKey);
            if (existing.isPresent()) {
                InvocationResult first = existing.get();
                InvocationResult replayView =
                        InvocationResult.replay(invocationId, first, elapsedMillis(start));
                log.info("[调用判定] invocationId={} function={}:{} 判定路径=幂等回放 key={} 实例={} 原始状态={} 呈现状态={}",
                        invocationId, functionName, actualVersion, idempotencyKey,
                        first.instanceId(), first.originStatus(), replayView.status());
                return replayView;
            }
        }

        // 3. 容量/并发判定：遗留模式走函数级信号量；新版本由实例池原子判定
        Semaphore legacySlots = null;
        if (definition.legacy()) {
            legacySlots = legacyConcurrencySlots.computeIfAbsent(functionName,
                    k -> new Semaphore(maxConcurrencyPerFunction));
            if (!legacySlots.tryAcquire()) {
                InvocationResult rejected = failure(invocationId, functionName, actualVersion,
                        InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED,
                        "concurrency limit exceeded for function '" + functionName + "'",
                        start, null);
                log.info("[调用判定] invocationId={} function={}:{} 判定路径=遗留函数并发到顶，立即拒绝不排队",
                        invocationId, functionName, actualVersion);
                rejectIdempotentKeyIfNeeded(idempotencyKey, idempotent, rejected);
                return rejected;
            }
        }

        InstanceManager.AcquireResult acquired = instanceManager.acquire(definition);
        if (acquired.kind == InstanceManager.AcquireResult.Kind.REJECTED) {
            InvocationResult rejected = failure(invocationId, functionName, actualVersion,
                    InvocationStatus.INSTANCE_CAPACITY_EXCEEDED,
                    "instance capacity (maxWarmInstances=" + definition.capacity().maxWarmInstances()
                            + ", maxConcurrencyPerInstance="
                            + definition.capacity().maxConcurrencyPerInstance()
                            + ") exceeded for function '" + functionName + "' version " + actualVersion,
                    start, null);
            log.info("[调用判定] invocationId={} function={}:{} 判定路径=版本暖实例/实例内并发到顶，立即拒绝不排队",
                    invocationId, functionName, actualVersion);
            if (legacySlots != null) {
                legacySlots.release();
            }
            rejectIdempotentKeyIfNeeded(idempotencyKey, idempotent, rejected);
            return rejected;
        }
        if (acquired.kind == InstanceManager.AcquireResult.Kind.CREATE_FAILED) {
            Throwable cause = acquired.cause == null ? new RuntimeException("instance create failed")
                    : acquired.cause;
            InvocationResult createError = failure(invocationId, functionName, actualVersion,
                    InvocationStatus.HANDLER_ERROR,
                    "instance initialization failed: "
                            + cause.getClass().getSimpleName() + ": " + cause.getMessage(),
                    start, null);
            log.info("[调用判定] invocationId={} function={}:{} 判定路径=冷启动实例创建失败 status={}",
                    invocationId, functionName, actualVersion, createError.status());
            if (legacySlots != null) {
                legacySlots.release();
            }
            if (idempotent) {
                // 冷启动失败属于真实执行失败，结果保留在幂等键中（与执行体异常语义一致）
                idempotencyStore.complete(idempotencyKey, createError);
            }
            return createError;
        }

        // 4. 在具体实例上执行（可能冷启动），并在超时约束下控制
        InstanceManager.Lease lease = acquired.lease;
        FunctionHandler handler = lease.handler();
        InvocationResult result;
        boolean instanceHealthy = false;
        try {
            result = execute(invocationId, functionName, actualVersion, handler, input, lease, start);
            // 超时/执行出错 => 实例立即淘汰；成功 => 保留为可复用暖实例
            instanceHealthy = result.status() == InvocationStatus.SUCCESS;
            // 5. 记录幂等结果，唤醒同键等待者
            if (idempotent) {
                idempotencyStore.complete(idempotencyKey, result);
            }
            return result;
        } finally {
            instanceManager.release(lease, instanceHealthy);
            if (legacySlots != null) {
                legacySlots.release();
            }
        }
    }

    private void rejectIdempotentKeyIfNeeded(String idempotencyKey, boolean idempotent,
                                             InvocationResult rejected) {
        if (idempotent) {
            // 容量/并发拒绝属于“执行前拒绝”：告知同键等待者后驱逐键，
            // 等待者看到的是拒绝状态（不算成功），后续同键调用可重新尝试真正执行
            idempotencyStore.completeAndEvict(idempotencyKey, rejected);
        }
    }

    private InvocationResult execute(String invocationId, String functionName, int version,
                                     FunctionHandler handler, Object input,
                                     InstanceManager.Lease lease, long start) {
        boolean coldStart = lease.coldStart();
        String instanceId = lease.instanceId();
        Future<Object> future = executor.submit(() -> handler.handle(input));
        try {
            Object value = future.get(invokeTimeoutMillis, TimeUnit.MILLISECONDS);
            log.info("[调用判定] invocationId={} function={}:{} 实例={} 判定路径={} status=SUCCESS",
                    invocationId, functionName, version, instanceId,
                    coldStart ? "冷启动新实例" : "复用暖实例");
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.SUCCESS, value, null, coldStart, elapsedMillis(start), instanceId);
        } catch (TimeoutException e) {
            future.cancel(true);
            log.info("[调用判定] invocationId={} function={}:{} 实例={} 判定路径=执行超时，实例将淘汰 status=TIMEOUT",
                    invocationId, functionName, version, instanceId);
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.TIMEOUT, null,
                    "invocation timed out after " + invokeTimeoutMillis + " ms",
                    coldStart, elapsedMillis(start), instanceId);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            log.info("[调用判定] invocationId={} function={}:{} 实例={} 判定路径=执行体抛异常，实例将淘汰 status=HANDLER_ERROR",
                    invocationId, functionName, version, instanceId);
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.HANDLER_ERROR, null,
                    cause.getClass().getSimpleName() + ": " + cause.getMessage(),
                    coldStart, elapsedMillis(start), instanceId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            log.info("[调用判定] invocationId={} function={}:{} 实例={} 判定路径=执行被中断，实例将淘汰 status=HANDLER_ERROR",
                    invocationId, functionName, version, instanceId);
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.HANDLER_ERROR, null, "invocation interrupted",
                    coldStart, elapsedMillis(start), instanceId);
        }
    }

    private InvocationResult failure(String invocationId, String functionName, int version,
                                     InvocationStatus status, String message, long start, String instanceId) {
        return InvocationResult.of(invocationId, functionName, version, status,
                null, message, false, elapsedMillis(start), instanceId);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    public void setInvokeTimeoutMillis(long invokeTimeoutMillis) {
        this.invokeTimeoutMillis = invokeTimeoutMillis;
    }

    public void setMaxConcurrencyPerFunction(int maxConcurrencyPerFunction) {
        this.maxConcurrencyPerFunction = maxConcurrencyPerFunction;
        legacyConcurrencySlots.clear();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
