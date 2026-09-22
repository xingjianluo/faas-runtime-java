package com.github.xingjianluo.faasruntimejava.faas;

/**
 * 发布结果分类：成功或给出可区分的失败原因。
 */
public enum PublishStatus {
    /** 发布成功，版本立即可见且可用 */
    PUBLISHED,
    /** 同一函数同名版本已存在，历史版本不可覆盖 */
    DUPLICATE_VERSION,
    /** 并发发布引发的状态竞争，本次发布被拒绝，可安全重试 */
    CONCURRENT_MODIFICATION,
    /** 发布参数非法（函数名为空、版本号非正、处理器为空等） */
    INVALID_ARGUMENT
}
