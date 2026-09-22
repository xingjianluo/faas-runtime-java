package com.github.xingjianluo.faasruntimejava.model;

import java.time.Duration;

/**
 * 发布时固化的函数级资源配置快照。历史版本被调用时始终使用发布时的这份配置。
 *
 * @param timeout            单次调用超时
 * @param concurrencyLimit   函数级并发上限（>=1）
 * @param idleTtl            实例空闲多久后允许缩容
 */
public record FunctionConfig(Duration timeout, int concurrencyLimit, Duration idleTtl) {
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(3);
    public static final int DEFAULT_CONCURRENCY = 10;
    public static final Duration DEFAULT_IDLE_TTL = Duration.ofSeconds(30);

    public FunctionConfig {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (concurrencyLimit < 1) {
            throw new IllegalArgumentException("concurrencyLimit must be >= 1");
        }
        if (idleTtl == null || idleTtl.isNegative()) {
            throw new IllegalArgumentException("idleTtl must not be negative");
        }
    }

    public static FunctionConfig defaults() {
        return new FunctionConfig(DEFAULT_TIMEOUT, DEFAULT_CONCURRENCY, DEFAULT_IDLE_TTL);
    }
}
