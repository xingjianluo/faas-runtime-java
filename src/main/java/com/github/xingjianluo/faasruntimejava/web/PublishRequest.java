package com.github.xingjianluo.faasruntimejava.web;

/** 发布请求体（骨架）。 */
public record PublishRequest(String name, Integer version, String handlerType,
                             Long timeoutMs, Integer concurrency, Long idleTtlMs) {
}
