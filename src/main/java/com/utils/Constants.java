package com.utils;

/**
 * Created by zhangweimin on 17/5/15.
 */
public class Constants {

    public final static int ERR_TYPE = -1;
    public final static int SUM_TYPE = 1;
    public final static int MIN_TYPE = 10;
    public final static int MAX_TYPE = 11;
    public final static int DISTINCT_TYPE = 12;

    public final static double ERROR_QUANTILE = -1.0;
    public final static double ERROR_MAXMIN_PREV = -1.0;
    public final static long ERROR_DISTINCT_VALUE = -1;
    public final static int MAX_MIN_LIMIT_SAMPLE_TIME = 10;
    public final static int DISTINC_LIMIT_SAMPLE_TIME = 10;

    public final static String SUM_TAG = "sum";
    public final static String COUNT = "count";
    public final static String SUM = "sum";
    public final static String DISTINCT = "distinct";
    public final static String AVG = "avg";
    public final static String VAR = "var";
    public final static String STD = "std";

    // SQL key words
    public final static String SELECT = "select";
    public final static String FROM = "from";
    public final static String WHERE = "where";
    public final static String MAX = "max";
    public final static String MIN = "min";
    public final static String CONFIDENCE = "confidence";
    public final static String GROUPBY = "group by";

    public final static String COL_PREFIX = "R";
    public final static String SAMPLE = "sample";

    public final static String MIN_MAX_MAP_KEY = "mmkey";
    public final static String RIGHT_BRACKETS = ")";

    // hdfs
    public final static String LOCALHOST = "local";
    public final static String LOCAL_HDFS_DOMAIN = "hdfs://localhost:9000";
    public final static String hdfsPathPrefix = "hdfs://localhost:9000";
    public final static String TABLE_SUFFIX = ".tbl";

    // default value
    public final static double DEFAULT_SAMPLE_RATE = 0.4;
    public final static int DEFAULT_COL_IDX = 0;
    public final static double LEAST_QUANTILE = 0.95;
    public final static double GROUPBY_LEAST_QUANTILE = 0.7;
    public final static double LEAST_DISTINCT_QUANTILE = 0.95;
    public final static double DEFAULT_CONFIDENCE = 0.95;

}
