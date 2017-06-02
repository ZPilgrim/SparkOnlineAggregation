package com.client;

import com.funcs.CountOperator;
import com.funcs.OnlineAggregationOperation;
import com.funcs.SumOperator;
import com.utils.Constants;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Main Interface for spark access and query execution.
 * @author Qiao Jin
 */
public class SparkOnlineAggregationClient implements Closeable {
    // see how to use: http://blog.csdn.net/luoweifu/article/details/46495045
    private final static Logger logger = Logger.getLogger(SparkOnlineAggregationClient.class.getName());

    // Spark context.
    private SparkSession spark;
    private JavaSparkContext context;
    // Input file path from Spark.
    private String inputFilePath;

    public SparkOnlineAggregationClient(String master, String inputFilePath) {
        this.inputFilePath = inputFilePath;

        init(master);
    }

    // Constructor to launch the spark online aggregation locally.
    public SparkOnlineAggregationClient(String inputFilePath) {
        this(Constants.LOCALHOST, inputFilePath);
    }

    public void init(String master) {
        // Spark configuration.
        spark = SparkSession
                .builder()
                .appName("Spark online aggregation")
                .master(master)
                .getOrCreate();
        context = new JavaSparkContext(spark.sparkContext());
        logger.info("Initialize Spark client configuration successfully!");
    }

    /**
     * Sample data source with given fraction and option.
     * @param withReplacement whether it will put back the sampled data, use false if you want don't produce duplications.
     * @param fraction the ratio of data to be sampled.
     * @return the Sampled data with wrapper of JavaRDD.
     */
    public JavaRDD<String> sample(JavaRDD<String> dataSource, boolean withReplacement, double fraction) {
        JavaRDD<String> sampledData = dataSource.sample(withReplacement, fraction);
        logger.info(String.format("Finish sampling data with fraction of %f%%", fraction * 100));

        return sampledData;
    }

    public Object execQuery(String query, double sampleFraction) {
        if (query == null || query.trim().length() == 0) {
            logger.severe("Invalid query is not ALLOWED! please try again.");
            return null;
        }
        // Get raw data from HDFS.
        JavaRDD<String> dataSource = context.textFile(inputFilePath);

        JavaRDD<String> sampledDataSource = sample(dataSource, false /* not with replacement */, sampleFraction);

        OnlineAggregationOperation operator = null;

        String[] queryTokens = query.split(" +");
        if (queryTokens == null || queryTokens.length < 4) {
            return null;
        }

        // Extract actual method name from the query.
        String aggregationMethodName = queryTokens[1].contains("(") && queryTokens[1].contains("")
                ? queryTokens[1].substring(0, queryTokens[1].indexOf("("))
                : queryTokens[1];

        String aggregationMethodNameLowerCase = aggregationMethodName.toLowerCase();

        // Runtime cast based on operationName.
        if (aggregationMethodNameLowerCase.equals(Constants.SUM)) {
            operator = new SumOperator(sampledDataSource);
        } else if (aggregationMethodNameLowerCase.equals(Constants.COUNT)) {
            operator = new CountOperator(sampledDataSource);
        } else {  // Add other operator handlers below.
            logger.severe("No valid operator recognized, return nothing.");
            // TODO Capture this logic?
            return null;
        }

        // Execute Aggregation.
        Object rslt = operator.exec(query);
        if (rslt == null) {
            return null;
        }
        // Display the operation result based on the current sampling.
        operator.showResult();

        return rslt;
    }

    /**
     * Will throw some error once this is executed, which is ignorable.
     * <p> See https://stackoverflow.com/questions/28362341/error-utils-uncaught-exception-in-thread-sparklistenerbus
     */
    @Override
    public void close() throws IOException {
        spark.close();
    }

    public static void main(String[] args) {
        String inputFilePath = "hdfs://localhost:9000/spark/lineitem.tbl";
        SparkOnlineAggregationClient client = new SparkOnlineAggregationClient(inputFilePath);

        String query = "select sum(R4) from T1" ;
        client.execQuery(query, 1);
    }
}