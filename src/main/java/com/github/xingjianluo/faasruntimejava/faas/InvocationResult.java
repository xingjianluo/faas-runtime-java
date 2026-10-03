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
     * （IDEMPOTENT_REPLAY）才算成功。容量/并发被拒、超时、执行异常、
     * 以及失败结果的幂等回放（IDEMPOTENT_REPLAY_FAILURE）一律不算成功。
     */
    public boolean success() {
        return status == InvocationStatus.SUCCESS || status == InvocationStatus.IDEMPOTENT_REPLAY;
    }

    /**
     * 是否属于“幂等回放”（不论首次成败）：命中已完成的幂等键、未重复执行副作用。
     * 与容量/并发被拒（执行前拒绝，非回放）严格区分。
     */
    public boolean replayed() {
        return status == InvocationStatus.IDEMPOTENT_REPLAY
                || status == InvocationStatus.IDEMPOTENT_REPLAY_FAILURE;
    }

    public static InvocationResult of(String invocationId, String functionName, int version,
                                      InvocationStatus status, Object value, String errorMessage,
                                      boolean coldStart, String instanceId, long durationMillis) {
        return new InvocationResult(invocationId, functionName, version,
                status, value, errorMessage, coldStart, instanceId, durationMillis);
    }
}
