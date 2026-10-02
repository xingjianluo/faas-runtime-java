package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 一次调用的完整结果，包含分类状态、返回值/错误信息、冷启动标识以及处理实例。
 *
 * @param status       本次调用对外呈现的分类：
 *                     <ul>
 *                       <li>真正执行成功为 {@link InvocationStatus#SUCCESS}；</li>
 *                       <li>命中幂等键且首次结果为成功时为 {@link InvocationStatus#IDEMPOTENT_REPLAY}；</li>
 *                       <li>回放的若是超时/执行异常/容量拒绝，则直接呈现首次的原始状态，
 *                           被拒结果不会被伪装成成功回放；</li>
 *                       <li>执行前拒绝（找不到函数、容量到顶等）保留对应拒绝状态。</li>
 *                     </ul>
 * @param instanceId   实际执行本次调用的实例 ID；执行前拒绝（找不到函数、容量到顶等）为 null；
 *                     幂等回放时透传首次执行实例的 ID
 * @param originStatus 幂等回放时首次调用的原始状态；非回放场景与 {@link #status} 相同
 * @param replayed     是否为幂等回放（未真正执行），用于在结果分类之外保留可识别性
 */
public record InvocationResult(
        String invocationId,
        String functionName,
        int version,
        InvocationStatus status,
        Object value,
        String errorMessage,
        boolean coldStart,
        long durationMillis,
        String instanceId,
        InvocationStatus originStatus,
        boolean replayed) {

    /**
     * 是否为调用方视角的成功：真正成功，或回放首次成功结果，才算成功；
     * 回放的若是超时/执行异常/容量拒绝，则仍不算成功。
     */
    public boolean success() {
        return status == InvocationStatus.SUCCESS
                || (status == InvocationStatus.IDEMPOTENT_REPLAY
                && originStatus == InvocationStatus.SUCCESS);
    }

    /** 真正执行（非回放）时使用的工厂方法 */
    public static InvocationResult of(String invocationId, String functionName, int version,
                                      InvocationStatus status, Object value, String errorMessage,
                                      boolean coldStart, long durationMillis, String instanceId) {
        return new InvocationResult(invocationId, functionName, version, status,
                value, errorMessage, coldStart, durationMillis, instanceId, status, false);
    }

    /**
     * 幂等回放视图：
     * <ul>
     *   <li>首次成功：呈现 {@link InvocationStatus#IDEMPOTENT_REPLAY}；</li>
     *   <li>首次失败/被拒：直接呈现首次的原始状态（被拒结果清清楚楚，不算成功），
     *       并通过 {@link #replayed} 保留回放可识别性。</li>
     * </ul>
     */
    public static InvocationResult replay(String invocationId, InvocationResult first, long durationMillis) {
        InvocationStatus presented = first.originStatus == InvocationStatus.SUCCESS
                ? InvocationStatus.IDEMPOTENT_REPLAY
                : first.originStatus;
        return new InvocationResult(invocationId, first.functionName(), first.version(),
                presented, first.value(), first.errorMessage(),
                false, durationMillis, first.instanceId(), first.originStatus(), true);
    }
}
