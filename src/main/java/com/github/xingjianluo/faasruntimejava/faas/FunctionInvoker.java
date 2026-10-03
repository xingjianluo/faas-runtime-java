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
 * 调用执行器：负责超时控制、并发/容量约束、异常归类、实例故障淘汰与幂等回放。
 *
 * <p>两类容量模式：
 * <ul>
 *   <li><b>老函数（发布时未配置容量）</b>：维持原行为——每版本一个暖实例，
 *       函数级全局信号量（默认 16）约束并发，抢不到返回
 *       {@link InvocationStatus#CONCURRENCY_LIMIT_EXCEEDED}；</li>
 *   <li><b>显式配置容量的版本</b>：按 {@link CapacityConfig} 管理多个暖实例，
 *       每个实例独立并发槽位；全部实例打满且实例数已达上限时立即返回
 *       {@link InvocationStatus#INSTANCE_CAPACITY_EXCEEDED}，不排队。</li>
 * </ul>
 *
 * <p>幂等与容量交叉的判定：
 * <ul>
 *   <li>同一幂等键并发进入，只有一个调用真正执行，其余等待并拿到同一结果；</li>
 *   <li>首次执行<b>成功</b>时，回放包装为 {@link InvocationStatus#IDEMPOTENT_REPLAY}，
 *       保留可识别性，且按成功计；</li>
 *   <li>首次执行<b>失败</b>（超时/异常）时，回放包装为
 *       {@link InvocationStatus#IDEMPOTENT_REPLAY_FAILURE}：与首次在成败上严格一致，
 *       可识别为回放，但绝不算成功；</li>
 *   <li>若首个调用因容量/并发被拒，等待者拿到的是<b>原始被拒状态</b>
 *       （不包装成回放、不算成功），键同时被驱逐，后续同键调用可重新尝试。</li>
 * </ul>
 */
@Component
public class FunctionInvoker {

    private static final Logger log = LoggerFactory.getLogger(FunctionInvoker.class);

    private final FunctionRegistry registry;
    private final InstanceManager instanceManager;
    private final IdempotencyStore idempotencyStore;

    /** 老函数的函数级并发槽位：函数名 -> 信号量 */
    private final ConcurrentMap<String, Semaphore> concurrencySlots = new ConcurrentHashMap<>();
    /** 执行函数的线程池：虚拟线程，超时通过中断取消 */
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** 单次调用超时，默认 5000ms，测试可调 */
    private volatile long invokeTimeoutMillis = 5_000;
    /** 老函数的函数级并发上限，默认 16，测试可调 */
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
     * @param version        版本号，<= 0 表示当前生效版本
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
        FunctionRegistry.Registration registration =
                registry.lookupRegistration(functionName, actualVersion).orElseThrow();
        FunctionHandler handler = registration.handler();
        CapacityConfig capacity = registration.capacity();
        boolean capacityConfigured = capacity != null;

        // 2. 幂等登记：同键调用等待首个结果，不重复执行
        boolean idempotent = idempotencyKey != null && !idempotencyKey.isBlank();
        if (idempotent) {
            var existing = idempotencyStore.beginOrAwait(idempotencyKey);
            if (existing.isPresent()) {
                InvocationResult first = existing.get();
                if (IdempotencyStore.isPreExecutionRejection(first)) {
                    // 被拒不是成功回放：保持原始被拒状态，且不算成功
                    log.info("[调用判定] invocationId={} 同键={} 等待到的是容量/并发被拒，返回原始被拒状态={}",
                            invocationId, idempotencyKey, first.status());
                    return InvocationResult.of(invocationId, functionName, actualVersion,
                            first.status(), null, first.errorMessage(),
                            false, null, elapsedMillis(start));
                }
                if (IdempotencyStore.isExecutionFailure(first)) {
                    // 首次执行失败：回放必须保持失败语义，可识别为回放但绝不算成功
                    log.info("[调用判定] invocationId={} 同键={} 回放首次失败结果 首次状态={} 判定路径=失败回放",
                            invocationId, idempotencyKey, first.status());
                    return InvocationResult.of(invocationId, functionName, actualVersion,
                            InvocationStatus.IDEMPOTENT_REPLAY_FAILURE, null, first.errorMessage(),
                            first.coldStart(), first.instanceId(), elapsedMillis(start));
                }
                return InvocationResult.of(invocationId, functionName, actualVersion,
                        InvocationStatus.IDEMPOTENT_REPLAY, first.value(), first.errorMessage(),
                        first.coldStart(), first.instanceId(), elapsedMillis(start));
            }
        }

        // 3. 获取执行资格：老函数走函数级信号量；显式容量版本走实例池
        Semaphore slots = null;
        InstanceManager.AcquireResult acquire;
        try {
            if (capacityConfigured) {
                acquire = instanceManager.acquire(functionName, actualVersion,
                        capacity, maxConcurrencyPerFunction, handler);
                if (!acquire.granted()) {
                    return rejectForCapacity(invocationId, functionName, actualVersion, start,
                            idempotencyKey, idempotent, InvocationStatus.INSTANCE_CAPACITY_EXCEEDED,
                            acquire.rejectReason());
                }
            } else {
                slots = concurrencySlots.computeIfAbsent(functionName,
                        k -> new Semaphore(maxConcurrencyPerFunction));
                if (!slots.tryAcquire()) {
                    InvocationResult rejected = failure(invocationId, functionName, actualVersion,
                            InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED,
                            "concurrency limit exceeded for function '" + functionName + "'", start);
                    if (idempotent) {
                        // 拒绝结果告知等待者后驱逐键，后续同键调用可重新尝试
                        idempotencyStore.completeAndEvict(idempotencyKey, rejected);
                    }
                    log.info("[调用判定] invocationId={} 函数={} 判定路径=老函数并发上限，拒绝 status={}",
                            invocationId, functionName, rejected.status());
                    return rejected;
                }
                acquire = instanceManager.acquire(functionName, actualVersion,
                        null, maxConcurrencyPerFunction, handler);
                if (!acquire.granted()) {
                    slots.release();
                    return rejectForCapacity(invocationId, functionName, actualVersion, start,
                            idempotencyKey, idempotent, InvocationStatus.CONCURRENCY_LIMIT_EXCEEDED,
                            acquire.rejectReason());
                }
            }
        } catch (RuntimeException initFailure) {
            // 冷启动初始化（如构建实例私有状态）失败：释放已占用槽位，归类为 HANDLER_ERROR
            if (slots != null) {
                slots.release();
            }
            InvocationResult error = failure(invocationId, functionName, actualVersion,
                    InvocationStatus.HANDLER_ERROR,
                    "instance init failed: " + initFailure.getClass().getSimpleName()
                            + ": " + initFailure.getMessage(), start);
            if (idempotent) {
                idempotencyStore.complete(idempotencyKey, error);
            }
            return error;
        }

        // 4. 在超时约束下于指定实例执行；出错/超时的实例随释放被淘汰
        InstanceManager.Lease lease = acquire.lease();
        InvocationResult result = null;
        try {
            result = execute(invocationId, functionName, actualVersion, handler, input, lease, start);
        } finally {
            boolean faulty = false;
            if (result != null) {
                faulty = result.status() == InvocationStatus.TIMEOUT
                        || result.status() == InvocationStatus.HANDLER_ERROR;
            }
            instanceManager.release(functionName, actualVersion, lease.instanceId(), faulty);
            if (slots != null) {
                slots.release();
            }
        }

        // 5. 记录幂等结果，唤醒同键等待者
        if (idempotent) {
            idempotencyStore.complete(idempotencyKey, result);
        }
        return result;
    }

    /** 组装容量/并发拒绝结果，并按幂等规则驱逐键 */
    private InvocationResult rejectForCapacity(String invocationId, String functionName, int version,
                                               long start, String idempotencyKey,
                                               boolean idempotent, InvocationStatus status,
                                               String reason) {
        InvocationResult rejected = failure(invocationId, functionName, version, status, reason, start);
        if (idempotent) {
            idempotencyStore.completeAndEvict(idempotencyKey, rejected);
        }
        log.info("[调用判定] invocationId={} 函数={}.v{} 判定路径=容量到顶立即拒绝（不排队）status={} 原因={}",
                invocationId, functionName, version, status, reason);
        return rejected;
    }

    private InvocationResult execute(String invocationId, String functionName, int version,
                                     FunctionHandler handler, Object input,
                                     InstanceManager.Lease lease, long start) {
        InstanceContext context = new InstanceContext(
                lease.instanceId(), functionName, version, lease.state());
        boolean stateful = handler instanceof StatefulFunctionHandler;
        Future<Object> future = executor.submit(() -> {
            if (stateful) {
                return ((StatefulFunctionHandler) handler).handle(input, context);
            }
            return handler.handle(input);
        });
        try {
            Object value = future.get(invokeTimeoutMillis, TimeUnit.MILLISECONDS);
            log.info("[调用判定] invocationId={} 函数={}.v{} 实例={} 判定路径=执行成功 coldStart={}",
                    invocationId, functionName, version, lease.instanceId(), lease.coldStart());
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.SUCCESS, value, null,
                    lease.coldStart(), lease.instanceId(), elapsedMillis(start));
        } catch (TimeoutException e) {
            future.cancel(true);
            log.warn("[调用判定] invocationId={} 函数={}.v{} 实例={} 判定路径=超时，实例将被淘汰",
                    invocationId, functionName, version, lease.instanceId());
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.TIMEOUT, null,
                    "invocation timed out after " + invokeTimeoutMillis + " ms",
                    lease.coldStart(), lease.instanceId(), elapsedMillis(start));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            log.warn("[调用判定] invocationId={} 函数={}.v{} 实例={} 判定路径=执行异常 {}，实例将被淘汰",
                    invocationId, functionName, version, lease.instanceId(),
                    cause.getClass().getSimpleName());
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.HANDLER_ERROR, null,
                    cause.getClass().getSimpleName() + ": " + cause.getMessage(),
                    lease.coldStart(), lease.instanceId(), elapsedMillis(start));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            log.warn("[调用判定] invocationId={} 函数={}.v{} 实例={} 判定路径=执行被中断，实例将被淘汰",
                    invocationId, functionName, version, lease.instanceId());
            return InvocationResult.of(invocationId, functionName, version,
                    InvocationStatus.HANDLER_ERROR, null, "invocation interrupted",
                    lease.coldStart(), lease.instanceId(), elapsedMillis(start));
        }
    }

    private InvocationResult failure(String invocationId, String functionName, int version,
                                     InvocationStatus status, String message, long start) {
        return InvocationResult.of(invocationId, functionName, version, status,
                null, message, false, null, elapsedMillis(start));
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
