package com.github.xingjianluo.faasruntimejava.exception;

/** FaaS 运行时异常基类，每种失败原因对应一个可区分的子类。 */
public abstract class FaasException extends RuntimeException {
    protected FaasException(String message) { super(message); }
    protected FaasException(String message, Throwable cause) { super(message, cause); }

    /** 函数不存在（从未发布过任何版本）。 */
    public static final class FunctionNotFoundException extends FaasException {
        public FunctionNotFoundException(String functionName) {
            super("function not found: " + functionName);
        }
    }

    /** 指定的版本不存在。 */
    public static final class VersionNotFoundException extends FaasException {
        public VersionNotFoundException(String functionName, Integer version) {
            super("version not found: " + functionName + "#v" + version);
        }
    }

    /** 版本号重复发布：历史版本不可覆盖。 */
    public static final class VersionAlreadyExistsException extends FaasException {
        public VersionAlreadyExistsException(String functionName, Integer version) {
            super("version already exists and is immutable: " + functionName + "#v" + version);
        }
    }

    /** 版本号非法（必须为正整数且单调递增）。 */
    public static final class InvalidVersionException extends FaasException {
        public InvalidVersionException(String message) { super(message); }
    }

    /** 处理器类型未知或无法实例化。 */
    public static final class InvalidHandlerException extends FaasException {
        public InvalidHandlerException(String message, Throwable cause) { super(message, cause); }
    }

    /** 调用参数非法。 */
    public static final class InvalidInvocationException extends FaasException {
        public InvalidInvocationException(String message) { super(message); }
    }

    /** 超出函数级并发上限。 */
    public static final class ConcurrencyLimitExceededException extends FaasException {
        public ConcurrencyLimitExceededException(String functionName, int limit) {
            super("concurrency limit exceeded: " + functionName + " limit=" + limit);
        }
    }

    /** 单次调用超时。 */
    public static final class InvocationTimeoutException extends FaasException {
        public InvocationTimeoutException(String functionVersion, long timeoutMs) {
            super("invocation timeout: " + functionVersion + " timeoutMs=" + timeoutMs);
        }
    }

    /** 函数业务执行抛出异常。 */
    public static final class FunctionExecutionException extends FaasException {
        public FunctionExecutionException(String functionVersion, Throwable cause) {
            super("function execution failed: " + functionVersion + " cause=" + cause, cause);
        }
    }
}
