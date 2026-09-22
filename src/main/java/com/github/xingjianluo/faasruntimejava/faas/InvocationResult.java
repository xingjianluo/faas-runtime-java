package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 一次调用的完整结果，包含分类状态、返回值/错误信息以及冷启动标识。
 */
public record InvocationResult(
        String invocationId,
        String functionName,
        int version,
        InvocationStatus status,
        Object value,
        String errorMessage,
        boolean coldStart,
        long durationMillis) {

    public boolean success() {
        return status == InvocationStatus.SUCCESS || status == InvocationStatus.IDEMPOTENT_REPLAY;
    }

    public static InvocationResult of(String invocationId, String functionName, int version,
                                      InvocationStatus status, Object value, String errorMessage,
                                      boolean coldStart, long durationMillis) {
        return new InvocationResult(invocationId, functionName, version, status,
                value, errorMessage, coldStart, durationMillis);
    }
}
