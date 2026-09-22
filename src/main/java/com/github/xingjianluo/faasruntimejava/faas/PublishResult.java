package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 发布结果：成功时携带函数名与版本号，失败时携带可区分的原因。
 */
public record PublishResult(
        PublishStatus status,
        String functionName,
        int version,
        String message) {

    public boolean published() {
        return status == PublishStatus.PUBLISHED;
    }

    public static PublishResult published(String functionName, int version) {
        return new PublishResult(PublishStatus.PUBLISHED, functionName, version, "published");
    }

    public static PublishResult rejected(PublishStatus status, String functionName, int version, String message) {
        return new PublishResult(status, functionName, version, message);
    }
}
