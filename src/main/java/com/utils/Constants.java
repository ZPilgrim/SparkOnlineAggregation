package com.utils;

/**
 * Created by zhangweimin on 17/5/15.
 */
public class Constants {

    public final static int ERR_TYPE = -1;
    public final static int SUM_TYPE = 1;
    public final static int MIN_TYPE = 10;
    public final static int MAX_TYPE = 11;

    public final static double ERROR_QUANTILE = -1.0;
    public final static double ERROR_MAXMIN_PREV = -1.0;

    public final static String SUM_TAG = "sum";

    // SQL key words
    public final static String SELECT = "select";
    public final static String FROM = "from";
    public final static String WHERE = "where";
    public final static String MAX = "max";
    public final static String MIN = "min";

    public final static String COL_PREFIX = "R";
    public final static String SAMPLE = "sample";

    public final static String MIN_MAX_MAP_KEY = "mmkey";
    public final static String RIGHT_BRACKETS = ")";

    // hdfs
    public final static String LOCALHOST = "local";
    public final static String LOCAL_HDFS_DOMAIN = "hdfs://localhost:9000";
    public final static String hdfsPathPrefix = "hdfs://localhost:9000";

    // default value
    public final static double DEFAULT_SAMPLE_RATE = 0.4;
    public final static int DEFAULT_COL_IDX = 0;
    public final static double LEAST_QUANTILE = 0.95;
}
