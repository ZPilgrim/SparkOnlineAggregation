package com.client;

/**
 * Created by zhangweimin on 17/5/15.
 */

import com.funcs.DistinctOperator;
import com.funcs.MaxMinOperator;
import com.funcs.CountOperator;
import com.funcs.OnlineAggregationOperation;
import com.funcs.SumOperator;
import com.utils.Constants;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkOnlineAggregationClient {
    // see how to use: http://blog.csdn.net/luoweifu/article/details/46495045
    static Logger logger = Logger.getLogger(SparkOnlineAggregationClient.class.getName());

    public SparkOnlineAggregationClient() {

    }

    public static void main(String[] args) {
        logger.setLevel(Level.FINEST);
        SparkOnlineAggregationClient client = new SparkOnlineAggregationClient();

        String query = args[0];

        client.execQuery(query);
    }

    public Object execQuery(String query) {

        OnlineAggregationOperation operator = null;
        Object rslt = null;

        if (query.indexOf(Constants.SUM_TAG) >= 0) {
            operator = new SumOperator();
            rslt = operator.exec(query);
        }
        if (query.indexOf(Constants.COUNT) >= 0) {
            operator = new CountOperator();
            rslt = operator.exec(query);
        }

        // other op
        if (query.indexOf(Constants.MAX) >= 0 ||
                query.indexOf(Constants.MIN) >= 0) {
            operator = new MaxMinOperator();
            rslt = operator.exec(query);
        }

        if (query.indexOf(Constants.DISTINCT) >= 0) {
            operator = new DistinctOperator();
            rslt = operator.exec(query);
        }

        if (query.indexOf(Constants.AVG) >= 0) {
            operator = new AvgOperator();
            rslt = operator.exec(query);
        }

        if (query.indexOf(Constants.VAR) >= 0) {
            operator = new VarOperator();
            rslt = operator.exec(query);
        }

        if (query.indexOf(Constants.STD) >= 0) {
            operator = new StdOperator();
            rslt = operator.exec(query);
        }

        if (operator != null) {
            operator.showResult();
        }

        return rslt;

    }
}