package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 调用结果分类：每种类别对应一种可区分的失败/成功原因，不允许混用。
 */
public enum InvocationStatus {
    /** 调用成功 */
    SUCCESS,
    /** 命中幂等键，直接返回首次执行的结果，未重复执行副作用 */
    IDEMPOTENT_REPLAY,
    /** 单次执行超时 */
    TIMEOUT,
    /** 函数执行体抛出异常 */
    HANDLER_ERROR,
    /** 超出函数级并发上限，调用被拒绝 */
    CONCURRENCY_LIMIT_EXCEEDED,
    /** 入参非法（如函数名为空、版本号非法、超时配置非法等） */
    INVALID_ARGUMENT,
    /** 函数不存在 */
    FUNCTION_NOT_FOUND,
    /** 函数存在但指定版本不存在 */
    VERSION_NOT_FOUND
}
