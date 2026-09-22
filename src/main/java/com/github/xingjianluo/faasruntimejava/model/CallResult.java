package com.github.xingjianluo.faasruntimejava.model;

import java.time.Instant;

/**
 * 调用结果。不同失败原因映射为不同 {@link InvocationStatus}，并携带判定依据。
 *
 * @param status         结果分类
 * @param functionName   实际命中的函数名
 * @param version        实际命中的版本（解析失败时可能为 null）
 * @param body           成功时的返回值
 * @param errorCode      机器可区分的失败原因码
 * @param errorMessage   人类可读的失败说明
 * @param coldStart      本次是否冷启动（新建实例）
 * @param replay         本次是否命中幂等重放（未重复执行副作用）
 * @param instanceId     服务实例标识
 * @param invocationId   本次调用标识
 * @param completedAt   结果产生时刻
 */
public record CallResult(InvocationStatus status, String functionName, Integer version,
                         String body, String errorCode, String errorMessage,
                         boolean coldStart, boolean replay, String instanceId,
                         String invocationId, Instant completedAt) {

    public boolean succeeded() {
        return status == InvocationStatus.SUCCESS || status == InvocationStatus.REPLAY;
    }
}
