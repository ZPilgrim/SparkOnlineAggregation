package com.structs;

import com.utils.Constants;

/**
 * Created by zhangweimin on 17/6/4.
 */
public class MaxMinResult {
    private double quantile;
    private double preV;
    public MaxMinResult(double preV, double quantile) {
        this.preV = preV;
        this.quantile = quantile;
    }
    public double getQuantile() { return quantile; }
    public double getPreV() { return preV; }
}
