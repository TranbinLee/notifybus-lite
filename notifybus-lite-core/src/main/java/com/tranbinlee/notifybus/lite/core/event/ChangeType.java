package com.tranbinlee.notifybus.lite.core.event;

/**
 * 资源变更类型，业务事实，不隐含任何"应该怎么处理"的建议。
 */
public enum ChangeType {

    /** 资源被创建。 */
    CREATED,

    /** 资源被更新。 */
    UPDATED,

    /** 资源被删除。 */
    DELETED,

    /** 资源被启用。 */
    ENABLED,

    /** 资源被禁用。 */
    DISABLED
}
