package com.utils;

/**
 * Created by zhangweimin on 17/6/3.
 */
public class StrUtils {

    public static boolean isInteger(String s) {
        if (s.length() == 0) return false;
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDouble(String s) {
        if (s.length() == 0) return false;
        int dot_cnt = 0;
        for (int i = 0; i < s.length(); i++) {
            if (Character.isDigit(s.charAt(i)) ) {
                continue;
            } else {
                if (s.charAt(i) == '.') {
                    dot_cnt += 1;
                } else {
                    return false;
                }
            }
        }
        if (dot_cnt > 1) return false;
        return true;
    }
}
