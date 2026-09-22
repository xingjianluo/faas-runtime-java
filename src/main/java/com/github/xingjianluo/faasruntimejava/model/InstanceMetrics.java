package com.github.xingjianluo.faasruntimejava.model;

/**
 * 某个函数版本的实例与调用指标快照（可复现、可解释）。
 */
public record InstanceMetrics(String functionName, int version,
                              int createdInstances, int idleInstances, int inflight,
                              long totalCalls, long successCalls, long replayCalls,
                              long timeoutCalls, long functionErrorCalls,
                              long concurrencyRejectedCalls, long coldStarts) {
}
