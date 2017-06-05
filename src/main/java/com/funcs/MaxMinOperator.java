package com.funcs;

/**
 * Created by zhangweimin on 17/5/21.
 */

import com.client.SparkOnlineAggregationClient;
import com.structs.MaxMinResult;
import com.utils.Constants;
import com.utils.DataFatcher;
import com.utils.StrUtils;
import com.utils.Timer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

import java.util.logging.Logger;

public class MaxMinOperator implements OnlineAggregationOperation {

    static Logger logger = Logger.getLogger(SparkOnlineAggregationClient.class.getName());

    // init use default value
    private double sampleRate = Constants.DEFAULT_SAMPLE_RATE;
    private int opType = Constants.MAX_TYPE;
    private String filePath = null;
    private int col_idx = Constants.DEFAULT_COL_IDX;
    private MaxMinResult lastResult = null;
    private SparkSession spark = null;
    private String lastSQL = null;
    private double leastQuantile = Constants.LEAST_QUANTILE;
    private Timer timmer = new Timer();

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

    // SQL format: select max(R1) from <filepath>
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
                if (i >= cols.length) break;
                cols[i].replace(" ", "");
                String[] opCols = cols[i].split(",");
                String opCol = opCols[0];

                if (opCol.indexOf(Constants.MAX) >= 0) {
                    this.opType = Constants.MAX_TYPE;
                } else {
                    if (opCol.indexOf(Constants.MIN) >= 0) {
                        this.opType = Constants.MIN_TYPE;
                    } else {
                        logger.info("min max operation  parse sql error:" + query + " not min or max");
                        return false;
                    }
                }

                parseColIdx(opCol);
                System.out.println("get col:" + cols[0]);
                i += 1;
                if (i >= cols.length) break;
            }

            if (cols[i].equalsIgnoreCase(Constants.FROM)) {
                i += 1;
                if (i >= cols.length) break;
                cols[i].replace(" ", "");
                this.filePath = cols[i].substring(1, cols[i].length() - 1);
                logger.finest("set filePath:" + this.filePath);
                i += 1;
                if (i >= cols.length) break;
            }

            if (cols[i].equalsIgnoreCase(Constants.SAMPLE)) {
                i += 1;
                if (i >= cols.length) break;
                cols[i].replace(" ", "");
                if (StrUtils.isDouble(cols[i])) {
                    this.sampleRate = Double.parseDouble(cols[i]);
                } else {
                    logger.severe("parse sample rate error:" + cols[i]);
                    return false;
                }
                i += 1;
                if (i >= cols.length) break;
            }

            if (cols[i].equalsIgnoreCase(Constants.CONFIDENCE)) {
                i += 1;
                if (i >= cols.length) break;
                cols[i].replace(" ", "");
                if (StrUtils.isDouble(cols[i])) {
                    this.leastQuantile = Double.parseDouble(cols[i]);
                } else {
                    logger.severe("parse sample rate error:" + cols[i]);
                    return false;
                }
                i += 1;
                if (i >= cols.length) break;
            }
        }
        return true;
    }

    private double calQuantile(double sega2, double dev_err, double preV, double avg, double avg_err) {
        double d = (sega2 * sega2 + dev_err * dev_err + Math.pow((preV - avg - avg_err), 2));
        if (d == 0) return Constants.ERROR_QUANTILE;
        return (sega2 * sega2 + dev_err * dev_err) / d;
    }

    private double evaluateResult(double avg, double dev, int cnt, double preV) {

        if (cnt <= 0 || cnt == 1) {
            System.err.println("Sample cnt less than 1:" + cnt);
            return Constants.ERROR_QUANTILE;
        }

        double t_n4 = Math.pow(dev, 2);
        double sega2 = dev / (double) (cnt - 1);
        double avg_err = Math.pow(Math.pow(1.96, 2) * sega2 / cnt, 0.5);
        double sega4 = t_n4 / (cnt - 1);
        double dev_err = Math.pow(
                Math.pow(1.96, 2) * (sega4 - Math.pow(sega2, 2)) / cnt, 0.5
        );
        double retOpRes = Constants.ERROR_QUANTILE;

        if (this.opType == Constants.MAX_TYPE) {
            retOpRes = 1.0 - calQuantile(sega2, dev_err, preV, avg, avg_err);
        } else {
            if (this.opType == Constants.MIN_TYPE) {
                retOpRes = 1.0 - calQuantile(sega2, dev_err, preV, avg, avg_err);
            } else {
                logger.severe("Operation type not support:" + this.opType);
            }
        }

        return retOpRes;
    }

    public MaxMinResult calResult(JavaRDD<String> data, final int col_idx) {
        return calResult(data, col_idx, 0.0, 0.0, 0);
    }

    public MaxMinResult calResult(JavaRDD<String> data, final int col_idx, double lastSum1, double lastSum2, int lastCnt) {

        if (data == null || col_idx < 0) {
            System.err.println("calResult check error");
            return new MaxMinResult(Constants.ERROR_MAXMIN_PREV, Constants.ERROR_QUANTILE);
        }

        JavaPairRDD<String, Tuple4<Double, Double, Integer, Double>> ones = data.mapToPair(
                new PairFunction<String, String, Tuple4<Double, Double, Integer, Double>>() {
                    public Tuple2<String, Tuple4<Double, Double, Integer, Double>> call(String s) throws Exception {
                        String doubleS = s.split("\\|")[col_idx];
                        double v = Double.parseDouble(doubleS);
                        Tuple4<Double, Double, Integer, Double> retV = new Tuple4<Double, Double, Integer, Double>(
                                new Double(v),
                                new Double(v * v),
                                new Integer(1),
                                new Double(v));
                        return new Tuple2<String, Tuple4<Double, Double, Integer, Double>>(Constants.MIN_MAX_MAP_KEY, retV);
                    }
                });

        Tuple2<String, Tuple4<Double, Double, Integer, Double>> reduceResult = ones.reduce(new Function2<Tuple2<String, Tuple4<Double, Double, Integer, Double>>, Tuple2<String, Tuple4<Double, Double, Integer, Double>>, Tuple2<String, Tuple4<Double, Double, Integer, Double>>>() {
            public Tuple2<String, Tuple4<Double, Double, Integer, Double>> call(Tuple2<String, Tuple4<Double, Double, Integer, Double>> v1, Tuple2<String, Tuple4<Double, Double, Integer, Double>> v2) throws Exception {
                Double evaRes = opType == Constants.MAX_TYPE ? Math.max(v1._2()._4(), v2._2()._4()) : Math.min(v1._2()._4(), v2._2()._4());
                Tuple4<Double, Double, Integer, Double> retV = new Tuple4<Double, Double, Integer, Double>(
                        v1._2()._1() + v2._2()._1(), v1._2()._2() + v2._2()._2(), v1._2()._3() + v2._2()._3(), evaRes
                );
                return new Tuple2<String, Tuple4<Double, Double, Integer, Double>>("reduceResult", retV);
            }
        });

        Double evaluationRes = reduceResult._2()._4();
        double tot_sum1 = reduceResult._2()._1().doubleValue() + lastSum1;
        double tot_sum2 = reduceResult._2()._2().doubleValue() + lastSum2;
        int cnt = reduceResult._2()._3().intValue() + lastCnt;
        double avg = tot_sum1 / (double) cnt;
        double dev = tot_sum2 / (double) cnt - avg * avg;

//        System.out.println("Sample avg:" + avg + " dev:" + dev + " cnt:" + cnt);

        double retQuantile = evaluateResult(avg, dev, cnt, evaluationRes.doubleValue());

//        System.out.println("predict v:" + evaluationRes.doubleValue());
//        System.out.println("calQuantile:" + retQuantile);

        return new MaxMinResult(evaluationRes.doubleValue(), retQuantile, tot_sum1, tot_sum2, cnt);
    }

    public Object exec(String query) {

        timmer.reset();
        this.spark = SparkSession.builder().master("local").appName("OnlineAggregationOperation").getOrCreate();
        int tryTimes = 0;
        System.out.println("exec query:" + query);
        if (!parseSQL(query)) {
            System.err.println("parse sql err:" + query);
            return new MaxMinResult(Constants.ERROR_MAXMIN_PREV, Constants.ERROR_QUANTILE);
        }

        // TODO: check if ok,or set err result and ret, create inc
        DataFatcher dataFetcher = new DataFatcher(this.spark, this.filePath, this.sampleRate, false);
        if (!dataFetcher.checkInit()) {
            System.err.println("dataFetcher init error");
            return new MaxMinResult(Constants.ERROR_MAXMIN_PREV, Constants.ERROR_QUANTILE);
        }

        timmer.start();

        JavaRDD<String> data = dataFetcher.getNextRDDData();

        if (data == null) {
            System.out.println("already sample all data, but cannot satisfy confidence.");
            return new MaxMinResult(Constants.ERROR_MAXMIN_PREV, Constants.ERROR_QUANTILE);
        }

        lastResult = calResult(data, this.col_idx);
        lastSQL = query;

        while (lastResult.getQuantile() < Constants.LEAST_QUANTILE
                && tryTimes < Constants.MAX_MIN_LIMIT_SAMPLE_TIME) {
            showResult(false);
            data = dataFetcher.getNextRDDData();
            if (data == null) {
                System.out.println("Already sample all data, but cannot satisfy confidence. Will return best result ever.");
                return lastResult;
            }
            lastResult = calResult(data, this.col_idx, lastResult.getSum1(), lastResult.getSum2(), lastResult.getCnt());
        }

        return new MaxMinResult(lastResult.getPreV(), lastResult.getQuantile());
    }

    public void showResult() {
        showResult(true);
    }

    public void showResult(boolean res) {
        if (lastResult == null) {
            System.err.println("lastResult not init");
            lastResult = new MaxMinResult(Constants.ERROR_MAXMIN_PREV, Constants.ERROR_QUANTILE);
        }
        String msg;
        if (res) {
            msg = "====== SUCC ======";
        } else {
            msg = "====== NOT GOOD ======";
        }
        System.out.println(msg);
        System.out.println("   SQL:" + lastSQL);
        System.out.println("   predict v:" + lastResult.getPreV());
        System.out.println("   Quantile:" + lastResult.getQuantile());
        System.out.println("   Runtime:" + timmer.showTime());
        if (!res) {
            msg = "====== keep try ======";
        } else {
            msg = "====== END ======";
        }
        System.out.println(msg);
    }
}
