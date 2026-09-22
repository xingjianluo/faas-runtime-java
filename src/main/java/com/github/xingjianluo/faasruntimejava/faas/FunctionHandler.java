package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 函数处理器：发布函数时提供的执行体。
 * 输入输出均为 JSON 可序列化对象，执行体抛出的任何异常都会被运行时捕获并归类。
 */
@FunctionalInterface
public interface FunctionHandler {

    /**
     * 执行函数逻辑。
     *
     * @param input 调用入参（可为 null）
     * @return 函数返回值（可为 null）
     * @throws Exception 执行体内部异常，由运行时归类为 HANDLER_ERROR
     */
    Object handle(Object input) throws Exception;
}
