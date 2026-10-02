package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 函数版本的容量配置：暖实例池上限与单个实例的并发上限。
 *
 * @param maxWarmInstances          该版本最多保留/创建的实例数，必须为正
 * @param maxConcurrencyPerInstance 单个实例允许同时执行的调用数，必须为正
 */
public record CapacityConfig(int maxWarmInstances, int maxConcurrencyPerInstance) {

    public static CapacityConfig of(int maxWarmInstances, int maxConcurrencyPerInstance) {
        return new CapacityConfig(maxWarmInstances, maxConcurrencyPerInstance);
    }
}
