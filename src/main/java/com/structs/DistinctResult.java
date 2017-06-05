package com.structs;

import java.io.Serializable;

/**
 * Created by chengrui on 2017/6/5.
 */
public class DistinctResult implements Serializable {
    private double quantile;
    private long dv;

    public DistinctResult(double quantile, long dv) {
        this.quantile = quantile;
        this.dv = dv;
    }

    public double getQuantile() {
        return quantile;
    }

    public double getDv() {
        return dv;
    }

}
