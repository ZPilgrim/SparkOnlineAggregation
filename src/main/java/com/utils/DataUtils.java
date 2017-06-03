package com.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * Created by zhangweimin on 17/6/1.
 */
public class DataUtils {

    public static JavaRDD<String> sampleData(SparkSession spark, String filePath, double ratio) {
//        SparkSession spark = SparkSession.builder().master("local").appName("OnlineAggregationOperation").getOrCreate();
        JavaRDD lines = spark.read().textFile(filePath).javaRDD();

        JavaRDD<String> sampleData = lines.sample(false, ratio);

        return sampleData;
    }

//    public static
}
