package org.example.utils;

import org.slf4j.Logger;

public class LogUtils {
    public static void safeError(Logger logger, String message, Throwable e) {
        logger.error(message, e);
    }
}
