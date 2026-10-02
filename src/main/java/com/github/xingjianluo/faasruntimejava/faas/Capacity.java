package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 单个函数版本的容量配置：
 * <ul>
 *   <li>{@code maxWarmInstances}：该版本最多保留/同时运行多少个健康实例；</li>
 *   <li>{@code maxConcurrencyPerInstance}：单个实例允许同时执行多少个调用。</li>
 * </ul>
 *
 * <p>到顶后立即拒绝（{@link InvocationStatus#INSTANCE_CAPACITY_EXCEEDED}），不排队。
 * 未显式配置容量的老函数走遗留容量：单实例 + 函数级并发信号量，行为保持不变。
 */
public record Capacity(int maxWarmInstances, int maxConcurrencyPerInstance) {

    /** 显式配置容量但未给暖实例数时的默认值 */
    public static final int DEFAULT_MAX_WARM_INSTANCES = 4;
    /** 显式配置容量但未给单实例并发时的默认值 */
    public static final int DEFAULT_MAX_CONCURRENCY_PER_INSTANCE = 1;

    /** 遗留函数的容量：每个版本一个实例，实例内并发不限（由函数级信号量约束） */
    public static Capacity legacy() {
        return new Capacity(1, Integer.MAX_VALUE);
    }

    /**
     * 解析按版本给出的容量配置：null 项取默认值。
     */
    public static Capacity configured(Integer maxWarmInstances, Integer maxConcurrencyPerInstance) {
        int warm = maxWarmInstances == null ? DEFAULT_MAX_WARM_INSTANCES : maxWarmInstances;
        int perInstance = maxConcurrencyPerInstance == null
                ? DEFAULT_MAX_CONCURRENCY_PER_INSTANCE : maxConcurrencyPerInstance;
        return new Capacity(warm, perInstance);
    }
}
