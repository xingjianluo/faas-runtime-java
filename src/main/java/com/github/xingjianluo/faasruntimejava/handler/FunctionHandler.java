package com.github.xingjianluo.faasruntimejava.handler;

/**
 * 用户函数的统一执行接口。每个版本在发布时绑定一个独立实例，
 * 实例内部状态即“冷启动后复用”的载体。
 */
public interface FunctionHandler {

    /**
     * 执行函数。
     *
     * @param payload 输入
     * @return 输出
     * @throws Exception 业务异常，由运行时映射为 FUNCTION_ERROR
     */
    String handle(String payload) throws Exception;

    /** 冷启动初始化（构造实例时调用一次）。 */
    default void init() {
    }
}
