package com.github.xingjianluo.faasruntimejava.model;

import java.time.Instant;

/**
 * 不可变的函数版本定义。一旦发布即冻结：代码（handlerType）与资源配置都不可变，
 * 从而保证“历史版本被调用时的行为与发布时一致”。
 *
 * @param name        函数名
 * @param version     版本号（正整数，同一函数内单调递增、不可重复）
 * @param handlerType 处理器类型标识，由工厂解析
 * @param config      发布时固化的资源配置快照
 * @param publishedAt 发布时刻（用于审计与日志）
 */
public record FunctionDefinition(String name, int version, String handlerType,
                                 FunctionConfig config, Instant publishedAt) {
    public String qualifiedName() {
        return name + "#v" + version;
    }
}
