package com.funcs;

import com.utils.AttributeGroup;
import com.utils.Constants;
import com.utils.QueryParser;

import java.util.logging.Logger;

/**
 * Sum operation for the spark online aggregation.
 * @author Qiao Jin
 */
public class SumOperator implements OnlineAggregationOperation {

    private final static Logger logger = Logger.getLogger(SumOperator.class.getName());

    // Spark context.
    private final SparkSession sparkSession;

    private Double lastResult = null;

    public SumOperator(String masterEndPoint) {
        // Initialize Spark context.
        sparkSession = SparkSession
            .builder()
            .master(masterEndPoint)
            .appName("OnlineAggregationOperation")
            .getOrCreate();
    }

    /** Constructor for launching the Spark locally. */
    public SumOperator() {
        this(Constants.LOCALHOST);
    }


    public Object exec(String query) {
        AttributeGroup group = parser.parse(query);
        if (group == null) {
            return null;
        }

        Double rslt = new Double(3.14);

        lastResult = new Double( rslt.doubleValue() );

        return rslt;
    }

    public void showResult() {
        logger.info("show sum result:");
        if (lastResult == null) {
            System.out.println("No SUM operation executed, result is unavailable!");
        } else {
            System.out.println(String.format("The current SUM is: %d", lastResult.longValue())); 
        }
    }

    //================================================================================
    // Utility APIs.
    //================================================================================

    /**
     * Function to compute the Inverse Accumulate distribution function (Normsinv).
     * <p> See <a href=https://www.medcalc.org/manual/normsinv_function.php>Normsinv validation page</a>
     */
    public double getNormsinv(double prob) {
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