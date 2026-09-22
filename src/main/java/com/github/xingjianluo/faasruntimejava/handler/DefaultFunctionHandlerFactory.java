package com.github.xingjianluo.faasruntimejava.handler;

import com.github.xingjianluo.faasruntimejava.exception.FaasException;
import com.github.xingjianluo.faasruntimejava.handler.builtin.EchoHandler;
import com.github.xingjianluo.faasruntimejava.handler.builtin.FailingHandler;
import com.github.xingjianluo.faasruntimejava.handler.builtin.GlobalSideEffectHandler;
import com.github.xingjianluo.faasruntimejava.handler.builtin.SideEffectHandler;
import com.github.xingjianluo.faasruntimejava.handler.builtin.SleepHandler;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 内置 handlerType -> 实现类 的解析工厂。
 * 每次发布都会创建一个全新的处理器实例，从而保证：
 * 1) 不同版本之间互不共享内存状态；
 * 2) 解析/实例化在发布原子区内完成，版本可见即可用。
 */
public class DefaultFunctionHandlerFactory implements FunctionHandlerFactory {

    private final Map<String, Class<? extends FunctionHandler>> types = new LinkedHashMap<>();

    public DefaultFunctionHandlerFactory() {
        register("echo", EchoHandler.class);
        register("sleep", SleepHandler.class);
        register("failing", FailingHandler.class);
        register("sideEffect", SideEffectHandler.class);
        register("globalSideEffect", GlobalSideEffectHandler.class);
    }

    /** 允许额外注册自定义处理器类型。 */
    public final void register(String handlerType, Class<? extends FunctionHandler> type) {
        if (handlerType == null || handlerType.isBlank()) {
            throw new FaasException.InvalidHandlerException("handlerType must not be blank", null);
        }
        types.put(handlerType, type);
    }

    @Override
    public FunctionHandler create(String handlerType) {
        if (handlerType == null || handlerType.isBlank()) {
            throw new FaasException.InvalidHandlerException("handlerType must not be blank", null);
        }
        Class<? extends FunctionHandler> type = types.get(handlerType);
        if (type == null) {
            throw new FaasException.InvalidHandlerException("unknown handlerType: " + handlerType, null);
        }
        try {
            FunctionHandler handler = type.getDeclaredConstructor().newInstance();
            handler.init();
            return handler;
        } catch (ReflectiveOperationException e) {
            throw new FaasException.InvalidHandlerException(
                    "cannot instantiate handlerType: " + handlerType, e);
        }
    }
}
