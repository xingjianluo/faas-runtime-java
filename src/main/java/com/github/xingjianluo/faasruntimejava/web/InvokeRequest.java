package com.github.xingjianluo.faasruntimejava.web;

/** 调用请求体（骨架）。 */
public record InvokeRequest(Integer version, String payload, String idempotencyKey) {
}
