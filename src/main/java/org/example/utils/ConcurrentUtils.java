package org.example.utils;

import java.util.concurrent.TimeUnit;

public class ConcurrentUtils {
    public static void sleep(int time, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
