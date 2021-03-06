package com.funcs;

import com.utils.AttributeGroup;
import com.utils.Constants;
import com.utils.DataFatcher;
import com.utils.QueryParser;
import com.utils.Timer;

import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * Count operation for spark online aggregation.
 * @author Qiao Jin
 */
public class CountOperator implements OnlineAggregationOperation {

	private final static Logger logger = Logger.getLogger(CountOperator.class.getName());

	private final static double DEFAULT_CONFIDENCE = 0.9;

	private QueryParser parser = new QueryParser();

    // Spark context.
    private final SparkSession sparkSession;

    private boolean isInterrupted = false;

    // Global display features.
    private long numOfRecord = 0;
    private double epsilon = 0;
    private double confidence = DEFAULT_CONFIDENCE;

    // Timer object.
    private Timer timer = new Timer();

    public CountOperator(String masterEndPoint) {
        // Initialize Spark context.
        sparkSession = SparkSession
            .builder()
            .master(masterEndPoint)
            .appName("OnlineAggregationOperation")
            .getOrCreate();
    }

    /** Constructor for launching the Spark locally. */
    public CountOperator() {
        this(Constants.LOCALHOST);
    }

	@Override
	public Object exec(String query) {
        // Reset the timer.
        timer.reset();

		final AttributeGroup group = parser.parse(query);
        if (group == null) {
            return null;
        }

        try {
        	confidence = Double.parseDouble(group.getConfidenceInterval());
        } catch (Exception e) {
        	logger.warning(String.format("Failed to parse valid confidence interval from input, use default value %f", DEFAULT_CONFIDENCE));
        }

        double alpha = 1 - confidence;
        double zp = getNormsinv(alpha / 2 + confidence);

        // Parse all the needed features.
        String inputFilePath = group.getSource().substring(1, group.getSource().length() - 1);
        double sampleFraction = Constants.DEFAULT_SAMPLE_RATE;
        try {
        	sampleFraction = Double.parseDouble(group.getSampleFraction());
        } catch (Exception e) {
            logger.warning(String.format("Failed to parse valid sample fraction from input, use default value %f", Constants.DEFAULT_SAMPLE_RATE));
        }

        DataFatcher dataFetcher = new DataFatcher(sparkSession, inputFilePath, sampleFraction, false /* not with replacement */);
        if (!dataFetcher.checkInit()) {
            System.err.println("dataFetcher init error");
            System.exit(0);
        }

        timer.start();

        double powerSum = 0,
               sum = 0;
        // Sample iterations until satisfying the confidence interval or user interruption.
		while (!isInterrupted) {
			 // Sampling.
            final JavaRDD<String> sampleRecords = dataFetcher.getNextRDDData();

            if (sampleRecords == null) {
                break;
            }

            numOfRecord += sampleRecords.count();
            double sampleSum = sampleRecords.count();

            sum += sampleSum;
            powerSum += sampleSum * sampleSum;

            // Compute the current global average.
            double average = sum / (double)numOfRecord;

            // Compute confidence interval.
            double tn2v = (powerSum - (double)numOfRecord * average * average) / (double)(numOfRecord - 1);
            epsilon = Math.sqrt(zp * zp * tn2v / (double)numOfRecord);

            // Display the intermediate result.
            showResult();
		}

		return Long.valueOf(numOfRecord);
	}

	@Override
	public void showResult() {
		System.out.println("===================== show COUNT result =====================");
        System.out.println(String.format("Confidence: %f; Current COUNT: %d; Confidence Interval: [%d-%f, %d+%f]; duration: %s",
            confidence, numOfRecord, numOfRecord, epsilon, numOfRecord, epsilon, timer.showTime()));
	}

	//================================================================================
	// Utility APIs.
	//================================================================================

	public void interrupt() {
        isInterrupted = true;
    }

    public boolean isInterrupted() {
        return isInterrupted;
    }

	/*
     * Function to compute the Inverse Accumulate distribution function (Normsinv).
     * <p> See <a href=https://www.medcalc.org/manual/normsinv_function.php>Normsinv validation page</a>
     */
    private double getNormsinv(double prob) {
        double LOW = 0.02425,
               HIGH = 0.97575,
               q, r;

        double a[] = {-3.969683028665376e+01, 2.209460984245205e+02,
                      -2.759285104469687e+02, 1.383577518672690e+02,
                      -3.066479806614716e+01, 2.506628277459239e+00},
               b[] = {-5.447609879822406e+01, 1.615858368580409e+02,
                      -1.556989798598866e+02, 6.680131188771972e+01,
                      -1.328068155288572e+01},
               c[] = {-7.784894002430293e-03, -3.223964580411365e-01,
                      -2.400758277161838e+00, -2.549732539343734e+00,
                      4.374664141464968e+00,  2.938163982698783e+00},
               d[] = {7.784695709041462e-03,  3.224671290700398e-01,
                      2.445134137142996e+00,  3.754408661907416e+00};

        if (prob < LOW) {
            q = Math.sqrt(-2 * Math.log(prob));
            return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4])
                    * q + c[5])
                    / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
        } else if (prob > HIGH) {
            q = Math.sqrt(-2 * Math.log(1 - prob));
            return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4])
                    * q + c[5])
                    / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
        } else {
            q = prob - 0.5;
            r = q * q;
            return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4])
                    * r + a[5])
                    * q
                    / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4])
                            * r + 1);
        }
    }
}