package com.funcs;

import com.client.SparkOnlineAggregationClient;
import com.structs.DistinctResult;
import com.utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Logger;


/**
 * Created by chengrui on 2017/5/20.
 */
public class DistinctOperator implements OnlineAggregationOperation {
    static Logger logger = Logger.getLogger(SparkOnlineAggregationClient.class.getName());
    private double sampleRate = Constants.DEFAULT_SAMPLE_RATE;
    private int opType = Constants.DISTINCT_TYPE;
    private String filePath = null;
    private int col_idx = Constants.DEFAULT_COL_IDX;
    DistinctResult lastResult = null;
    private SparkSession spark = null;
    private String lastSQL = null;
    private double leastQuantile = Constants.LEAST_DISTINCT_QUANTILE;
    long startTime = 0;

    private boolean parseColIdx(String colName) {
        if (colName == null) return false;
        int ridx = colName.indexOf(Constants.COL_PREFIX);
        if (ridx < 0) return false;
        String numStr = colName.substring(ridx + 1, colName.indexOf(Constants.RIGHT_BRACKETS));

        if (StrUtils.isInteger(numStr)) {
            this.col_idx = Integer.parseInt(numStr);
            return true;
        }
        logger.severe("parseColIdx error, parseColIdx:" + colName);

        return false;
    }

    private boolean parseSQL(String query) {
        String[] cols = query.split(" ");
        for (int i = 0; i < cols.length; i++) {
            cols[i] = cols[i].trim();
        }

        int i = 0;
        while (i < cols.length) {
            // select
            if (cols[i].equalsIgnoreCase(Constants.SELECT)) {
                i += 1;
                cols[i].replace(" ", "");
                String[] opCols = cols[i].split(",");
                String opCol = opCols[0];

                if (opCol.indexOf(Constants.DISTINCT) >= 0) {
                    this.opType = Constants.DISTINCT_TYPE;
                } else {
                    logger.info("min max operation  parse sql error:" + query + " no distinct");
                    return false;
                }

                parseColIdx(opCol);
                System.out.println("get col:" + cols[0]);
                i += 1;

                if (i >= cols.length)
                    break;
            }

            if (cols[i].equalsIgnoreCase(Constants.FROM)) {
                i += 1;
                cols[i].replace(" ", "");
                this.filePath = cols[i].substring(1, cols[i].length() - 1);
                logger.finest("set filePath:" + this.filePath);
                i += 1;
                if (i >= cols.length)
                    break;
            }

            if (cols[i].equalsIgnoreCase(Constants.SAMPLE)) {
                i += 1;
                if (i >= cols.length)
                    break;
                cols[i].replace(" ", "");
                if (StrUtils.isDouble(cols[i])) {
                    this.sampleRate = Double.parseDouble(cols[i]);
                } else {
                    logger.severe("parse sample rate error:" + cols[i]);
                    return false;
                }
                i += 1;
                if (i >= cols.length)
                    break;
            }

            if (cols[i].equalsIgnoreCase(Constants.CONFIDENCE)) {
                i += 1;
                if (i >= cols.length)
                    break;
                cols[i].replace(" ", "");
                if (StrUtils.isDouble(cols[i])) {
                    this.leastQuantile = Double.parseDouble(cols[i]);
                } else {
                    logger.severe("parse sample rate error:" + cols[i]);
                    return false;
                }
                i += 1;
                if (i >= cols.length)
                    break;
            }
        }
        return true;
    }

    public DistinctResult calResult(JavaRDD<String> data, final int col_idx) {
        JavaRDD<String> values;
        JavaPairRDD<String, Integer> ones;
        JavaPairRDD<String, Integer> tillNowDistinctValues;

        values = data.flatMap(new GetAttribute(col_idx));
        ones = values.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, Integer.valueOf(1));
            }
        });

        tillNowDistinctValues = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return 1;
            }
        });
        long m = tillNowDistinctValues.count(); //tillNowDistinctValue

        List<String> valueList = values.collect();
        FlajoletMartin fm = new FlajoletMartin(64, 3, 4);
        long dv = fm.countUniqueWords(valueList);
        double pro = 1 - Math.pow(Math.E, 0 - m * Math.pow(2, 0-fm.r));
        DistinctResult result = new DistinctResult(pro, dv);
        return result;
    }


        public Object exec(String query) {
        this.startTime=System.currentTimeMillis();

        this.spark = SparkSession.builder().master("local").appName("OnlineAggregationOperation").getOrCreate();
        int tryTimes = 0;
        System.out.println("exec query:" + query);
        if (!parseSQL(query)) {
            System.err.println("parse sql err:" + query);
            return new DistinctResult(Constants.ERROR_QUANTILE, Constants.ERROR_DISTINCT_VALUE);
        }

        // TODO: check if ok,or set err result and ret, create inc
        DataFatcher dataFetcher = new DataFatcher(this.spark, this.filePath, this.sampleRate, false);
        if (!dataFetcher.checkInit()) {
            System.err.println("dataFetcher init error");
            return new DistinctResult(Constants.ERROR_QUANTILE, Constants.ERROR_DISTINCT_VALUE);
        }

        JavaRDD<String> data = dataFetcher.getNextRDDData();

        if (data == null) {
            System.out.println("already sample all data, but cannot satisfy confidence.");
            return new DistinctResult(Constants.ERROR_QUANTILE, Constants.ERROR_DISTINCT_VALUE);
        }

        lastResult = calResult(data, this.col_idx);
        lastSQL = query;

        while (lastResult.getQuantile() < Constants.LEAST_DISTINCT_QUANTILE
                    && tryTimes < Constants.DISTINC_LIMIT_SAMPLE_TIME) {
                showResult(false);
            JavaRDD<String> newData = dataFetcher.getNextRDDData();
            if (newData == null) {
                System.out.println("Already sample all data, but cannot satisfy confidence. Will return best result ever.");
                return lastResult;
            }
            data = data.union(newData);
            lastResult = calResult(data, this.col_idx);
        }

        return 0;
    }

    public void showResult() {
        showResult(true);
    }
    public void showResult(boolean res) {
        if (lastResult == null) {
            System.err.println("lastResult not init");
            lastResult = new DistinctResult(Constants.ERROR_QUANTILE, Constants.ERROR_DISTINCT_VALUE);
        }
        String msg;
        if (res) {
            msg = "====== SUCC ======";
        } else {
            msg = "====== NOT GOOD ======";
        }
        System.out.println(msg);
        System.out.println("   SQL:" + lastSQL);
        System.out.println("   Quantile:" + lastResult.getQuantile());
        System.out.println("   Predict ditinctValue:" + lastResult.getDv());
        System.out.println("   Runtime:" + (System.currentTimeMillis()-this.startTime) + "ms");
        if (!res) {
            msg = "====== keep try ======";
        } else {
            msg = "====== END ======";
        }
        System.out.println(msg);
    }

}
