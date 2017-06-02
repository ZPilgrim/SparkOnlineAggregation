package com.funcs;

import java.util.logging.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.utils.AttributeGroup;
import com.utils.QueryParser;

/**
 * Sum operation for the spark online aggregation.
 * @author Qiao Jin
 */
public class SumOperator implements OnlineAggregationOperation {

    private final static Logger logger = Logger.getLogger(SumOperator.class.getName());

    private Double lastResult;

    private QueryParser parser = new QueryParser();

    private JavaRDD<String> dataSource;

    public SumOperator(JavaRDD<String> dataSource) {
        this.dataSource = dataSource;
    }

    public Object exec(String query) {
        final AttributeGroup group = parser.parse(query);
        if (group == null) {
            return null;
        }

        lastResult = dataSource.map(new Function<String, Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(String s) throws Exception {
                int columnIndex;
                try {
                    columnIndex = Integer.parseInt(group.getColumnIndex());
                } catch (Exception e) {
                    logger.severe(String.format("Failed to parse column index %d.", group.getColumnIndex()));
                    // Ingnore the current data row.
                    return Double.valueOf(0);
                }

                return Double.parseDouble(s.split("\\|")[columnIndex]);
            }
        }).reduce(new Function2<Double, Double, Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double call(Double num1, Double num2) throws Exception {
                return num1 + num2;
            }
        });

        return lastResult;
    }

    public void showResult() {
        if (lastResult == null) {
            System.out.println("No Sum operation executed, result is unavailable!");
        } else {
            System.out.println(String.format("The current SUM is: %f", lastResult.doubleValue()));
        }
    }
}