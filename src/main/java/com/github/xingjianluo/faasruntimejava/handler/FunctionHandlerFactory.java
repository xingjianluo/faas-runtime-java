package com.github.xingjianluo.faasruntimejava.handler;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;

/**
 * 按发布时的 handlerType 解析并创建处理器实例。
 * 解析发生在“发布原子区内”，因此版本一旦可见，处理器必然可用，
 * 不会出现“版本可见却不可用”的中间状态。
 */
public interface FunctionHandlerFactory {

    /**
     * @throws FaasException.InvalidHandlerException 类型未知或实例化失败
     */
    FunctionHandler create(String handlerType);
}
