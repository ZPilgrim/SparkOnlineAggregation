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
public class AvgOperator implements OnlineAggregationOperation {

    static Logger logger = Logger.getLogger(AvgOperator.class.getName());

    private double avg;
    private double epsilon;

    private double confidence = Constants.DEFAULT_CONFIDENCE;
    String filePath;
    int columnIndex = Constants.DEFAULT_COL_IDX;
    double sampleFraction = Constants.DEFAULT_SAMPLE_RATE;

    private SparkSession spark = null;
    private Timer timmer = new Timer();

    // SQL format: select avg(R1) from <filepath> sample 0.1 confidence 0.95
    public boolean parseSQL(String query) {
        String[] cols = query.split("\\s+");
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].equalsIgnoreCase(Constants.SELECT)) {
                i++;
                if (i < cols.length && Constants.AVG.equalsIgnoreCase(cols[i].substring(0, 3))) {
                    String strColumnIndex = cols[i].substring(cols[i].indexOf("(") + 2, cols[i].indexOf(")"));
                    if (StrUtils.isDouble(strColumnIndex)) {
                        columnIndex = Integer.parseInt(strColumnIndex);
                    } else {
                        logger.severe("parse avg column error:" + strColumnIndex);
                        return false;
                    }
                } else {
                    logger.info("avg operation parse sql error:" + query + " not avg");
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

        double sum = 0, power2Sum = 0;
        Long count = new Long(0);
        while (true) {
            // sample
            final JavaRDD<String> sampleRecords = dataFetcher.getNextRDDData();

            // end condition
            if (sampleRecords == null) {
                break;
            }

            // compute average
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
            sum = sum + sampleSum;
            avg = sum / count;

            // compute interval
            double power2SampleSum = computePowerSampleSum(colValues, 2);
            power2Sum = power2Sum + power2SampleSum;
            double tn2uv = (power2Sum - count * avg * avg) / (double)(count - 1); // variance
            double tn11uvu = 0, tnu = 1, tnuv = avg, rn2 = tnuv / tnu, tn2u = 0; // just for clarity
            double gn = tn2uv - 2 * rn2 * tn11uvu + rn2 * rn2 * tn2u;
            epsilon = Math.sqrt(zp * zp * gn / (count * tnu * tnu));

            // show result
            showResult();
        }

        return avg;
    }

    public void showResult() {
        // TODO Auto-generated method stub
        System.out.println("==================Current result======================");
        System.out.println("Average：" + avg);
        System.out.println("Confidence：" + confidence);
        System.out.println("Epsilon：" + epsilon);
        System.out.println("Interval：[" + avg + "-" + epsilon + "," + avg + "+" + epsilon + "]");
        System.out.println("Runtime:" + timmer.showTime());
        System.out.println("======================================================");
        System.out.println();
    }
}
