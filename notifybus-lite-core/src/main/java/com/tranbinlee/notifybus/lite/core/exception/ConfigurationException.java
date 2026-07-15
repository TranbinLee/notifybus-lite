package com.tranbinlee.notifybus.lite.core.exception;

/**
 * 参数/配置非法，比如空白的 topic、resourceKey，或者非法的注解方法签名。
 */
public class ConfigurationException extends NotifyBusLiteException {

    private static final long serialVersionUID = 1L;

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
