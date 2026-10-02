package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 实例级处理器工厂：每次冷启动一个新实例时调用一次 {@link #create()}，
 * 产出的 {@link FunctionHandler} 仅属于该实例。
 *
 * <p>不同实例之间通过“各自 create 一份处理器”天然隔离实例级状态：
 * 处理器内部的字段就是该实例的执行上下文与内部状态，新实例无法看到
 * 其他实例留下的内容。
 */
@FunctionalInterface
public interface FunctionHandlerFactory {

    /**
     * 为一个新实例创建独立的处理器。
     *
     * @throws Exception 创建失败时由运行时归类为 HANDLER_ERROR，该实例不会进入暖实例池
     */
    FunctionHandler create() throws Exception;
}
