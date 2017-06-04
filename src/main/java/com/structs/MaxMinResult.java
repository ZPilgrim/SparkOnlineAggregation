package com.structs;

import java.io.Serializable;

/**
 * Created by zhangweimin on 17/6/4.
 */
public class MaxMinResult implements Serializable {
    private double quantile;
    private double preV;
    private double sum1 = 0.0;
    private double sum2 = 0.0;
    private int cnt = 0;

    public MaxMinResult(double preV, double quantile) {
        this.preV = preV;
        this.quantile = quantile;
    }

    public MaxMinResult(double preV, double quantile,
                        double sum1, double sum2, int cnt) {
        this.preV = preV;
        this.quantile = quantile;
        this.sum1 = sum1;
        this.sum2 = sum2;
        this.cnt = cnt;
    }

    public double getQuantile() {
        return quantile;
    }

    public double getPreV() {
        return preV;
    }

    public double getSum1() {
        return sum1;
    }

    public double getSum2() {
        return sum2;
    }

    public int getCnt() {
        return cnt;
    }
}
