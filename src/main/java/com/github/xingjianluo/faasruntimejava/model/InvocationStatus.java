package com.github.xingjianluo.faasruntimejava.model;

/** 调用结果的可区分分类，禁止用统一错误码掩盖不同失败原因。 */
public enum InvocationStatus {
    SUCCESS,
    REPLAY,
    TIMEOUT,
    FUNCTION_ERROR,
    CONCURRENCY_REJECTED,
    INVALID_ARGUMENT,
    VERSION_NOT_FOUND
}
