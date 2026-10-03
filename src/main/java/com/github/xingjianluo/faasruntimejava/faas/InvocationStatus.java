package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 调用结果分类：每种类别对应一种可区分的失败/成功原因，不允许混用。
 */
public enum InvocationStatus {
    /** 调用成功 */
    SUCCESS,
    /** 命中幂等键，直接回放首次执行的成功结果，未重复执行副作用 */
    IDEMPOTENT_REPLAY,
    /**
     * 命中幂等键，回放的是首次执行的失败结果（超时/执行异常）：
     * 与首次在成败上保持一致——不算成功，但仍是可识别的“回放”，
     * 与容量/并发被拒（从未执行）严格区分
     */
    IDEMPOTENT_REPLAY_FAILED,
    /** 单次执行超时 */
    TIMEOUT,
    /** 函数执行体抛出异常 */
    HANDLER_ERROR,
    /** 超出函数级并发上限（未配置容量的老函数），调用被拒绝 */
    CONCURRENCY_LIMIT_EXCEEDED,
    /** 暖实例池打满：所有实例的并发槽位都占用且实例数已达版本上限，立即拒绝不排队 */
    INSTANCE_CAPACITY_EXCEEDED,
    /** 入参非法（如函数名为空、版本号非法、超时配置非法等） */
    INVALID_ARGUMENT,
    /** 函数不存在 */
    FUNCTION_NOT_FOUND,
    /** 函数存在但指定版本不存在 */
    VERSION_NOT_FOUND
}
