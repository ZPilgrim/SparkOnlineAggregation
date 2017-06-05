package com.utils;

import java.io.Serializable;

/**
 * Created by zhangweimin on 17/6/6.
 */
public class Timer implements Serializable {
    private long time;

    public Timer() {
        this.time = 0;
    }

    public void reset() {
        this.time = 0;
    }

    public void start() {
        this.time = System.currentTimeMillis();
    }

    public long getTime() {
        return System.currentTimeMillis() - time;
    }

    public String showTime() {
        return Long.toString(System.currentTimeMillis() - time) + " ms";
    }
}
