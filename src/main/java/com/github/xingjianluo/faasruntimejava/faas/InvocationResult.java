package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 一次调用的完整结果，包含分类状态、返回值/错误信息、冷启动标识与处理实例标识。
 */
public record InvocationResult(
        String invocationId,
        String functionName,
        int version,
        InvocationStatus status,
        Object value,
        String errorMessage,
        boolean coldStart,
        String instanceId,
        long durationMillis) {

    /**
     * 是否按“成功”计：只有真正执行成功（SUCCESS）或命中成功结果的幂等回放
     * （IDEMPOTENT_REPLAY）才算成功。容量/并发被拒、超时、执行异常一律不算成功。
     */
    public boolean success() {
        return status == InvocationStatus.SUCCESS || status == InvocationStatus.IDEMPOTENT_REPLAY;
    }

    public static InvocationResult of(String invocationId, String functionName, int version,
                                      InvocationStatus status, Object value, String errorMessage,
                                      boolean coldStart, String instanceId, long durationMillis) {
        return new InvocationResult(invocationId, functionName, version,
                status, value, errorMessage, coldStart, instanceId, durationMillis);
    }
}
