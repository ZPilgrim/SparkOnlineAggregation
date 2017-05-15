package com.funcs;

import com.client.SparkOnlineAggregationClient;

import java.util.logging.Logger;

/**
 * Created by zhangweimin on 17/5/15.
 */
public class SumOperator implements OnlineAggregationOperation {

    static Logger logger = Logger.getLogger(SumOperator.class.getName());

    Double lastResult = null;

    public Object exec(String query) {
        // TODO Auto-generated method stub

        Double rslt = new Double(3.14);

        lastResult = new Double( rslt.doubleValue() );

        return rslt;
    }

    public void showResult() {
        // TODO Auto-generated method stub
        logger.info("show sum result:");
        System.out.println( "Sum:" );
        System.out.println( lastResult.toString() );

    }

}