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

    /** 发布函数新版本，历史版本不可覆盖 */
    public PublishResult publish(String functionName, int version, FunctionHandler handler) {
        return registry.publish(functionName, version, handler);
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

    /** 当前活跃实例数 */
    public int activeInstances() {
        return instanceManager.activeInstances();
    }

    /** 运行时指标：活跃实例数、冷启动次数、暖实例命中次数 */
    public Map<String, Object> metrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("activeInstances", instanceManager.activeInstances());
        metrics.put("coldStarts", instanceManager.coldStartCount());
        metrics.put("warmHits", instanceManager.warmHitCount());
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
