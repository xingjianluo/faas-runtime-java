package com.github.xingjianluo.faasruntimejava.faas;

import jakarta.annotation.PreDestroy;
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
 * 调用执行器：负责超时控制、函数级并发上限、异常归类与幂等回放。
 *
 * <p>语义约定：
 * <ul>
 *   <li>每次调用受单次超时（默认 5s）与函数级并发上限（默认 16）约束；</li>
 *   <li>超时、执行体异常、并发超限、参数非法分别映射为
 *       {@link InvocationStatus#TIMEOUT} / {@link InvocationStatus#HANDLER_ERROR} /
 *       {@link InvocationStatus#CONCURRENCY_LIMIT_EXCEEDED} / {@link InvocationStatus#INVALID_ARGUMENT}；</li>
 *   <li>被拒绝或失败的调用不产生副作用：并发槽位与实例占用都在 finally 中释放，
 *       结果互不污染；</li>
 *   <li>参数与版本校验在幂等登记之前完成，非法调用不会占用幂等键。</li>
 * </ul>
 */
@Component
public class FunctionInvoker {

    private final FunctionRegistry registry;
    private final InstanceManager instanceManager;
    private final IdempotencyStore idempotencyStore;

    /** 函数级并发槽位：函数名 -> 信号量 */
    private final ConcurrentMap<String, Semaphore> concurrencySlots = new ConcurrentHashMap<>();
    /** 执行函数的线程池：虚拟线程，超时通过中断取消 */
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** 单次调用超时，默认 5000ms，测试可调 */
    private volatile long invokeTimeoutMillis = 5_000;
    /** 函数级并发上限，默认 16，测试可调 */
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
                    "functionName must not be blank", start);
        }
        if (!registry.containsFunction(functionName)) {
            return failure(invocationId, functionName, version, InvocationStatus.FUNCTION_NOT_FOUND,
                    "function '" + functionName + "' not found", start);
        }
        var resolvedVersion = registry.resolveVersion(functionName, version);
        if (resolvedVersion.isEmpty()) {
            return failure(invocationId, functionName, version, InvocationStatus.VERSION_NOT_FOUND,
                    "version " + version + " of function '" + functionName + "' not found", start);
        }
        int actualVersion = resolvedVersion.get();
        FunctionHandler handler = registry.lookup(functionName, actualVersion).orElseThrow();

        // 2. 幂等登记：同键调用直接回放首个结果，不重复执行
        boolean idempotent = idempotencyKey != null && !idempotencyKey.isBlank();
        if (idempotent) {
            var existing = idempotencyStore.beginOrAwait(idempotencyKey);
            if (existing.isPresent()) {
                InvocationResult first = existing.get();
                return InvocationResult.of(invocationId, first.functionName(), first.version(),
                        InvocationStatus.IDEMPOTENT_REPLAY, first.value(), first.errorMessage(),
                        first.coldStart(), elapsedMillis(start));
            }
        }

        // 3. 并发上限：抢不到槽位立即拒绝，不排队、不产生副作用
        Semaphore slots = concurrencySlots.computeIfAbsent(functionName,
                k -> new Semaphore(maxConcurrencyPerFunction));
        if (!slots.tryAcquire()) {
            InvocationResult rejected = failure(invocationId, functionName, actualVersion,
                    InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED,
                    "concurrency limit exceeded for function '" + functionName + "'", start);
            if (idempotent) {
                // 拒绝结果告知等待者后驱逐键，后续同键调用可重新尝试
                idempotencyStore.completeAndEvict(idempotencyKey, rejected);
            }
            return rejected;
        }

        // 4. 获取实例（可能冷启动）并在超时约束下执行
        boolean coldStart = instanceManager.acquire(functionName, actualVersion);
        InvocationResult result;
        try {
            result = execute(invocationId, functionName, actualVersion, handler, input, coldStart, start);
        } finally {
            instanceManager.release(functionName, actualVersion);
            slots.release();
        }

        // 5. 记录幂等结果，唤醒同键等待者
        if (idempotent) {
            idempotencyStore.complete(idempotencyKey, result);
        }
        return result;
    }

    private InvocationResult execute(String invocationId, String functionName, int version,
                                     FunctionHandler handler, Object input,
                                     boolean coldStart, long start) {
        Future<Object> future = executor.submit(() -> handler.handle(input));
        try {
            Object value = future.get(invokeTimeoutMillis, TimeUnit.MILLISECONDS);
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.SUCCESS, value, null, coldStart, elapsedMillis(start));
        } catch (TimeoutException e) {
            future.cancel(true);
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.TIMEOUT, null,
                    "invocation timed out after " + invokeTimeoutMillis + " ms",
                    coldStart, elapsedMillis(start));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.HANDLER_ERROR, null,
                    cause.getClass().getSimpleName() + ": " + cause.getMessage(),
                    coldStart, elapsedMillis(start));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.HANDLER_ERROR, null, "invocation interrupted",
                    coldStart, elapsedMillis(start));
        }
    }

    private InvocationResult failure(String invocationId, String functionName, int version,
                                     InvocationStatus status, String message, long start) {
        return InvocationResult.of(invocationId, functionName, version, status,
                null, message, false, elapsedMillis(start));
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    public void setInvokeTimeoutMillis(long invokeTimeoutMillis) {
        this.invokeTimeoutMillis = invokeTimeoutMillis;
    }

    public void setMaxConcurrencyPerFunction(int maxConcurrencyPerFunction) {
        this.maxConcurrencyPerFunction = maxConcurrencyPerFunction;
        concurrencySlots.clear();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
