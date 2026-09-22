package com.github.xingjianluo.faasruntimejava.instance;

import com.github.xingjianluo.faasruntimejava.handler.FunctionHandler;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 一个被预热并可复用的函数实例。处理器在发布时已构造好，
 * 实例化（冷启动）发生在首次调用获取实例时。
 *
 * <p>{@code lastUsedAt} 只在持有池锁时更新，读取用于缩容判定。
 */
public final class FunctionInstance {

    private static final AtomicLong ID_SEQ = new AtomicLong();

    private final String id;
    private final FunctionHandler handler;
    private final Instant createdAt;
    private volatile Instant lastUsedAt;

    public FunctionInstance(String qualifier, FunctionHandler handler, Instant createdAt) {
        this.id = qualifier + "-inst-" + ID_SEQ.incrementAndGet();
        this.handler = handler;
        this.createdAt = createdAt;
        this.lastUsedAt = createdAt;
    }

    public String id() {
        return id;
    }

    public FunctionHandler handler() {
        return handler;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant lastUsedAt() {
        return lastUsedAt;
    }

    public void touch() {
        this.lastUsedAt = Instant.now();
    }
}
