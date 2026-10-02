package com.github.xingjianluo.faasruntimejava.faas;

import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FaaS 运行时门面：对外的发布、调用与指标入口。
 */
@Service
public class FaasRuntime {

    private final FunctionRegistry registry;
    private final FunctionInvoker invoker;
    private final InstanceManager instanceManager;

    public FaasRuntime(FunctionRegistry registry, FunctionInvoker invoker,
                       InstanceManager instanceManager) {
        this.registry = registry;
        this.invoker = invoker;
        this.instanceManager = instanceManager;
    }

    /**
     * 老发布方式：共享处理器 + 遗留容量（未显式配置容量），行为与旧版一致。
     */
    public PublishResult publish(String functionName, int version, FunctionHandler handler) {
        return registry.publish(functionName, version, handler);
    }

    /**
     * 发布无状态函数版本：所有实例共享同一处理器，按版本配置容量。
     *
     * @param maxWarmInstances          该版本最多保留的暖实例数，null 取默认值
     * @param maxConcurrencyPerInstance 单实例允许的并发调用数，null 取默认值
     */
    public PublishResult publish(String functionName, int version, FunctionHandler handler,
                                 Integer maxWarmInstances, Integer maxConcurrencyPerInstance) {
        return registry.publish(functionName, version, handler,
                maxWarmInstances, maxConcurrencyPerInstance);
    }

    /**
     * 发布有状态函数版本：每个冷启动实例通过工厂独立创建处理器，实例级状态隔离。
     *
     * @param maxWarmInstances          该版本最多保留的暖实例数，null 取默认值
     * @param maxConcurrencyPerInstance 单实例允许的并发调用数，null 取默认值
     */
    public PublishResult publish(String functionName, int version, FunctionHandlerFactory factory,
                                 Integer maxWarmInstances, Integer maxConcurrencyPerInstance) {
        return registry.publish(functionName, version, factory,
                maxWarmInstances, maxConcurrencyPerInstance);
    }

    /**
     * 调用函数。
     *
     * @param version        <= 0 表示当前生效版本
     * @param idempotencyKey 幂等键，可为 null
     */
    public InvocationResult invoke(String functionName, int version, Object input, String idempotencyKey) {
        return invoker.invoke(functionName, version, input, idempotencyKey);
    }

    /** 当前健康暖实例数 */
    public int activeInstances() {
        return instanceManager.activeInstances();
    }

    /** 运行时指标：实例数、冷启动、暖命中、故障淘汰、容量拒绝及按版本的实例明细 */
    public Map<String, Object> metrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("activeInstances", instanceManager.activeInstances());
        metrics.put("drainingInstances", instanceManager.drainingInstances());
        metrics.put("coldStarts", instanceManager.coldStartCount());
        metrics.put("warmHits", instanceManager.warmHitCount());
        metrics.put("evictedInstances", instanceManager.evictedCount());
        metrics.put("capacityRejections", instanceManager.capacityRejectionCount());
        metrics.put("pools", instanceManager.poolSizes());
        return metrics;
    }

    // ---- 测试与本地调试用配置 ----

    public void setInvokeTimeoutMillis(long timeoutMillis) {
        invoker.setInvokeTimeoutMillis(timeoutMillis);
    }

    public void setMaxConcurrencyPerFunction(int maxConcurrency) {
        invoker.setMaxConcurrencyPerFunction(maxConcurrency);
    }

    public void setIdleTimeoutMillis(long idleTimeoutMillis) {
        instanceManager.setIdleTimeoutMillis(idleTimeoutMillis);
    }

    public void setReapIntervalMillis(long reapIntervalMillis) {
        instanceManager.setReapIntervalMillis(reapIntervalMillis);
    }
}
