package com.funcs;

import com.utils.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

/**
 * Created by niyihan on 2017/6/4.
 */
public class StdOperator implements OnlineAggregationOperation {

    static Logger logger = Logger.getLogger(StdOperator.class.getName());

    private double stdDev;
    private double epsilon;

    private double confidence = Constants.DEFAULT_CONFIDENCE;
    String filePath;
    int columnIndex = Constants.DEFAULT_COL_IDX;
    double sampleFraction = Constants.DEFAULT_SAMPLE_RATE;

    private SparkSession spark = null;
    private Timer timmer = new Timer();

    // SQL format: select std(R1) from <filepath> sample 0.1 confidence 0.95
    public boolean parseSQL(String query) {
        String[] cols = query.split("\\s+");
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].equalsIgnoreCase(Constants.SELECT)) {
                i++;
                if (i < cols.length && Constants.STD.equalsIgnoreCase(cols[i].substring(0, 3))) {
                    String strColumnIndex = cols[i].substring(cols[i].indexOf("(") + 2, cols[i].indexOf(")"));
                    if (StrUtils.isDouble(strColumnIndex)) {
                        columnIndex = Integer.parseInt(strColumnIndex);
                    } else {
                        logger.severe("parse std dev column error:" + strColumnIndex);
                        return false;
                    }
                } else {
                    logger.info("std dev operation parse sql error:" + query + " not std dev");
                    return false;
                }
            } else if (cols[i].equalsIgnoreCase(Constants.FROM)){
                i++;
                this.filePath = cols[i].substring(1, cols[i].length() - 1);
                logger.finest("set filePath:" + this.filePath);
            } else if (cols[i].equalsIgnoreCase(Constants.SAMPLE)) {
                i++;
                if (StrUtils.isDouble(cols[i])) {
                    this.sampleFraction = Double.parseDouble(cols[i]);
                } else {
                    logger.severe("parse sample rate error:" + cols[i]);
                    return false;
                }
            } else if (cols[i].equalsIgnoreCase(Constants.CONFIDENCE)) {
                i++;
                if (StrUtils.isDouble(cols[i])) {
                    this.confidence = Double.parseDouble(cols[i]);
                } else {
                    logger.severe("parse confidence error:" + cols[i]);
                    return false;
                }
            }
        }
        return true;
    }

    public double computePowerSampleSum(JavaRDD<Double> colValues, final int power) {
        JavaRDD<Double> powercolValues = colValues.map(new Function<Double, Double>() {
            public Double call(Double aDouble) throws Exception {
                return Math.pow(aDouble, power);
            }
        });
        double powerSampleSum = powercolValues.reduce(new Function2<Double, Double, Double>() {
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        });
        return powerSampleSum;
    }

    public Object exec(String query) {
        // TODO Auto-generated method stub

        timmer.reset();

        // parse sql query and extract parameter
        boolean legal = parseSQL(query);
        if (!legal) {
            System.err.println("parse sql err:" + query);
            System.exit(0);
        }

        // initiate
        this.spark = SparkSession.builder().master("local").appName("OnlineAggregationOperation").getOrCreate();
        DataFatcher dataFetcher = new DataFatcher(this.spark, this.filePath, this.sampleFraction, false);
        if (!dataFetcher.checkInit()) {
            System.err.println("dataFetcher init error");
            System.exit(0);
        }
        double alpha = 1 - confidence;
        double p = 1 - alpha / 2;
        double zp = new Normsinv().normsinv(p);

        // iteration

        timmer.start();

        double variance = 0, sum = 0, avg = 0, power2Sum = 0, power2Avg = 0, power3Sum = 0, power4Sum = 0;
        Long count = new Long(0);
        while (true) {
            // sample
            final JavaRDD<String> sampleRecords = dataFetcher.getNextRDDData();

            // end condition
            if (sampleRecords == null) {
                break;
            }

            // compute std dev
            count = count + sampleRecords.count();
            JavaRDD<Double> colValues = sampleRecords.map(new Function<String, Double>() {
                public Double call(String s) throws Exception {
                    String [] cols = s.split("\\|");
                    Double colValue = Double.parseDouble(cols[columnIndex]);
                    return colValue;
                }
            });
            double sampleSum = colValues.reduce(new Function2<Double, Double, Double>() {
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            });
            double power2SampleSum = computePowerSampleSum(colValues, 2);
            power2Sum = power2Sum + power2SampleSum;
            sum = sum + sampleSum;
            avg = sum / count;
            variance = (power2Sum - count * avg * avg) / (double)(count - 1);
            stdDev = Math.sqrt(variance);

            // compute interval
            double power4SampleSum = computePowerSampleSum(colValues, 4);
            power4Sum = power4Sum + power4SampleSum;
            power2Avg = power2Sum / count;

            double power3SampleSum = computePowerSampleSum(colValues, 3);
            power3Sum = power3Sum + power3SampleSum;

            double tn2uv2 =(power4Sum - count * power2Avg * power2Avg) / (double)(count - 1); // 即每个样本平方的方差
            double tnu = 1, tnuv = avg, rn2 = tnuv / tnu; // 仅为清晰起见列出，下同
            double tn11uv2uv = (power3Sum - avg * power2Sum - power2Avg * sum + count * avg * power2Avg) / (double)(count - 1);
            double tnuv2 = power2Avg, rn1 = tnuv2 / tnu;
            double tn11uv2u = 0, tn2uv = variance, tn11uvu = 0, tn2u = 0;
            double gn1 = tn2uv2 - 4 * rn2 * tn11uv2uv + (4 * rn2 * rn2 - 2 * rn1) * tn11uv2u + 4 * rn2 * rn2 * tn2uv
                    + (4 * rn1 * rn2 - 8 * rn2 * rn2 * rn2) * tn11uvu + Math.pow(4 * rn2 * rn2 - 2 * rn1, 2) * tn2u;
            double zn = variance;

            epsilon = Math.sqrt(zp * zp * gn1 / (4 * count * zn * tnu * tnu));

            // show result
            showResult();
        }

        return avg;
    }

    public void showResult() {
        // TODO Auto-generated method stub
        System.out.println("==================Current result======================");
        System.out.println("STD DEV：" + stdDev);
        System.out.println("Confidence：" + confidence);
        System.out.println("Epsilon：" + epsilon);
        System.out.println("Interval：[" + stdDev + "-" + epsilon + "," + stdDev + "+" + epsilon + "]");
        System.out.println("Runtime:" + timmer.showTime());
        System.out.println("======================================================");
        System.out.println();
    }
}
