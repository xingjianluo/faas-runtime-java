package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 实例执行上下文：每个暖实例独占一份，实例之间互不可见。
 *
 * @param instanceId 实例唯一标识（同函数同版本下唯一）
 * @param functionName 函数名
 * @param version 版本号
 * @param state 该实例的私有状态，由 {@link StatefulFunctionHandler#initInstanceState()} 创建
 */
public record InstanceContext(
        String instanceId,
        String functionName,
        int version,
        Object state) {
}
