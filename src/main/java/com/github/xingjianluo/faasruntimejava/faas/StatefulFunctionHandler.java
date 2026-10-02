package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 持有实例级状态的函数处理器。
 *
 * <p>每个暖实例冷启动时由运行时调用一次 {@link #initInstanceState()} 创建私有状态，
 * 该实例处理后续调用时通过 {@link InstanceContext#state()} 拿到同一份状态；
 * 不同实例（含同函数同版本的多个暖实例、不同版本的实例）状态互相隔离。
 */
public interface StatefulFunctionHandler extends FunctionHandler {

    /**
     * 冷启动时为新实例创建独立的私有状态。
     *
     * @return 实例私有状态，可为 null
     */
    Object initInstanceState();

    /**
     * 携带实例上下文执行函数逻辑。
     *
     * @param input   调用入参
     * @param context 当前实例的执行上下文（含私有状态与实例 ID）
     */
    Object handle(Object input, InstanceContext context) throws Exception;

    @Override
    default Object handle(Object input) throws Exception {
        throw new UnsupportedOperationException("stateful handler requires an instance context");
    }
}
