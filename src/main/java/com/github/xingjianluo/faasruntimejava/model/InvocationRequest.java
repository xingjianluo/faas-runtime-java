package com.github.xingjianluo.faasruntimejava.model;

/**
 * 一次函数调用请求。
 *
 * @param functionName  函数名（必填）
 * @param version       指定版本；为 null 时使用当前生效版本
 * @param payload       业务输入
 * @param idempotencyKey 幂等键；为 null 表示不做幂等去重
 */
public record InvocationRequest(String functionName, Integer version,
                                String payload, String idempotencyKey) {
}
