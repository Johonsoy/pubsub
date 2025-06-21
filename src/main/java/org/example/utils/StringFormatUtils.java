package org.example.utils;

public class StringFormatUtils {
    public static String format(String template, Object... args) {
        return String.format(template, args);
    }
}
