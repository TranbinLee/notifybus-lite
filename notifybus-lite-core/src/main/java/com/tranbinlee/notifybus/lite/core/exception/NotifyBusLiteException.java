package com.tranbinlee.notifybus.lite.core.exception;

/**
 * NotifyBus Lite 异常体系的根类。
 */
public class NotifyBusLiteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NotifyBusLiteException(String message) {
        super(message);
    }

    public NotifyBusLiteException(String message, Throwable cause) {
        super(message, cause);
    }
}
