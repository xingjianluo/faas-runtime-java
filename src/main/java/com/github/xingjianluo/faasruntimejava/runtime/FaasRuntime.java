package com.github.xingjianluo.faasruntimejava.runtime;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;
import com.github.xingjianluo.faasruntimejava.handler.FunctionHandlerFactory;
import com.github.xingjianluo.faasruntimejava.idempotency.IdempotencyStore;
import com.github.xingjianluo.faasruntimejava.instance.InstancePool;
import com.github.xingjianluo.faasruntimejava.invocation.InvocationService;
import com.github.xingjianluo.faasruntimejava.metrics.MetricsCollector;
import com.github.xingjianluo.faasruntimejava.model.CallResult;
import com.github.xingjianluo.faasruntimejava.model.FunctionDefinition;
import com.github.xingjianluo.faasruntimejava.model.InstanceMetrics;
import com.github.xingjianluo.faasruntimejava.model.InvocationRequest;
import com.github.xingjianluo.faasruntimejava.registry.FunctionRegistry;
import com.github.xingjianluo.faasruntimejava.scaling.IdleReaper;
import java.time.Duration;

/**
 * 对外门面：聚合发布、调用、缩容与指标查询。
 * 既可作为 Spring Bean 注入，也可在纯 Java 单测中直接 new。
 */
public final class FaasRuntime {

    private final FunctionRegistry registry;
    private final IdempotencyStore idempotencyStore;
    private final MetricsCollector metricsCollector;
    private final InvocationService invocationService;
    private final IdleReaper reaper;

    public FaasRuntime(FunctionHandlerFactory factory, RuntimeProperties properties) {
        this.registry = new FunctionRegistry(factory);
        this.idempotencyStore = new IdempotencyStore();
        this.metricsCollector = new MetricsCollector();
        this.invocationService = new InvocationService(registry, idempotencyStore, metricsCollector);
        this.reaper = new IdleReaper(registry);
    }

    /** 发布新版本；重复/越序版本与非法处理器会抛出可区分的 {@link FaasException}。 */
    public FunctionDefinition publish(String name, int version, String handlerType,
                                      Duration timeout, Integer concurrency, Duration idleTtl) {
        return registry.publish(name, version, handlerType, timeout, concurrency, idleTtl);
    }

    /** 执行调用，所有失败原因以不同 {@link CallResult#status()} 返回。 */
    public CallResult invoke(InvocationRequest request) {
        return invocationService.invoke(request);
    }

    /** 手动触发一轮空闲缩容，返回回收实例数。 */
    public int reapIdle() {
        return reaper.reapOnce();
    }

    /** 查询指标：version 为 null 时取当前生效版本。 */
    public InstanceMetrics metrics(String name, Integer version) {
        FunctionRegistry.ResolvedVersion resolved = registry.resolve(name, version);
        FunctionDefinition def = resolved.definition();
        InstancePool pool = resolved.pool();
        return metricsCollector.snapshot(def.name(), def.version(),
                pool.createdCount(), pool.idleCount(), pool.inflight());
    }

    public FunctionRegistry registry() {
        return registry;
    }

    public IdempotencyStore idempotencyStore() {
        return idempotencyStore;
    }

    public MetricsCollector metricsCollector() {
        return metricsCollector;
    }

    public IdleReaper reaper() {
        return reaper;
    }
}
