package com.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * Created by zhangweimin on 17/6/4.
 */

/*
 * We assume the datafiles are saved seperatedly.
 * For example, the file nation.tbl on the hdfs is saved as several seperated files the size of
 * which are same and the names are nation_0.tbl, nation_1.tbl ...
* */
public class DataFatcher {

    private int dataPtr;
    private SparkSession sparkSession;
    private boolean isInit = false;
    private String filePathPrefix;
    private String filePathSuffix;
    private boolean withReplacement;
    private double sampleRate;

    public DataFatcher(SparkSession sparkSession, String filePath, double ratio, boolean withReplacement) {

        if (sparkSession == null
                || filePath == null
                || ratio <= 0.0) {
            isInit = false;
            return;
        }

        this.dataPtr = 0;
        this.sampleRate = ratio;
        this.sparkSession = sparkSession;
        this.withReplacement = withReplacement;
        int idx = filePath.indexOf(Constants.TABLE_SUFFIX);
        if (idx < 0) {
            isInit = false;
            return;
        }

        this.filePathPrefix = filePath.substring(0, idx);
        this.dataPtr = -1;
        this.filePathSuffix = Constants.TABLE_SUFFIX;

        isInit = true;
    }

    public boolean checkInit() {
        return this.isInit;
    }

    private String makeFilePath(int idx) {
        return this.filePathPrefix + "_" + idx + this.filePathSuffix;
    }

    public String getCurFilePath() {
        return makeFilePath(this.dataPtr);
    }

    private String getNextFilePath() {
        this.dataPtr += 1;
        return makeFilePath(this.dataPtr);
    }

    // if necessary, user can change the ratio
    public JavaRDD<String> getNextRDDData(double ratio) {
        String filePath = getNextFilePath();
        // TODO: JUDGE if the file exists.
        try {
            JavaRDD lines = this.sparkSession.read().textFile(filePath).javaRDD();

            JavaRDD<String> sampleData = lines.sample(this.withReplacement, ratio);

            return sampleData;
        } catch (Exception e) {
            System.out.println("[WARN]: no more data");
            return null;
        }
    }

    public JavaRDD<String> getNextRDDData() {
        return getNextRDDData(this.sampleRate);
    }

    public JavaRDD<String> sample(JavaRDD<String> dataSource, boolean withReplacement, double fraction) {
        JavaRDD<String> sampledData = dataSource.sample(withReplacement, fraction);

        return sampledData;
    }
}
