package com.client;

/**
 * Created by zhangweimin on 17/5/15.
 */
import com.funcs.OnlineAggregationOperation;
import com.funcs.SumOperator;
import com.utils.Constants;
import java.util.logging.Logger;

public class SparkOnlineAggregationClient {
    // see how to use: http://blog.csdn.net/luoweifu/article/details/46495045
    static Logger logger = Logger.getLogger(SparkOnlineAggregationClient.class.getName());

    public SparkOnlineAggregationClient() {

    }

    public Object execQuery(String query) {

        OnlineAggregationOperation operator = null;
        Object rslt = null;

        if (query.indexOf(Constants.SUM) >= 0) {
            operator = new SumOperator();
            rslt = (Double) operator.exec(query);
        }

        // other op

        if (operator != null) {
            operator.showResult();
        }

        return rslt;

    }

    public static void main(String[] args) {
        SparkOnlineAggregationClient client = new SparkOnlineAggregationClient();
        String query = "select sum(R1) from T1" ;
        client.execQuery(query);
    }
}