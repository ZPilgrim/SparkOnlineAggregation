package com.funcs;

import com.utils.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import static com.utils.Constants.*;

/**
 * Created by xupeng on 2017/5/22.
 */
public class GroupByOperator implements Serializable,OnlineAggregationOperation{

    public static Logger logger = Logger.getLogger(GroupByOperator.class.getName());

//    final String SELECT = "select";
//    final String FROM = "from";
//    final String GROUPBY = "group by";

    public Map<String,List<String>> queryParse;
    double confidence = 0;
    public List<List<String>> rst = new ArrayList<List<String>>();

    public Object exec(String query){
        // TODO Auto-generated method stub
        queryParse = GroupByUtil.parseQuery(query);
        if(null == queryParse){
            return null;
        }
        final List<Integer> groupbyIndex = GroupByUtil.getLineIndex(queryParse.get(GROUPBY));
        if(null == groupbyIndex){
            return null;
        }
        final List<Integer> selectIndex = GroupByUtil.getLineIndex(queryParse.get(SELECT));
        if(null == selectIndex){
            return null;
        }

        //SparkConf conf = new SparkConf().setAppName("Group By");
        //JavaSparkContext jsc = new JavaSparkContext(conf);
        //SparkSession spark = SparkSession.builder().master("local").appName("OnlineAggregationOperation").getOrCreate();
        SparkSession sparkSession = SparkSession.builder().master("local").appName("OnlineAggregationOperation").getOrCreate();
        //System.out.println(queryParse.get(FROM).get(0));

        List<List<String>> curr = new ArrayList<List<String>>();
        double lower_bound = GROUPBY_LEAST_QUANTILE;
        if(0 != queryParse.get(CONFIDENCE).size()){
            lower_bound = Double.parseDouble(queryParse.get(CONFIDENCE).get(0));
        }
        System.out.println("lower_bound="+lower_bound);
        double sample = DEFAULT_SAMPLE_RATE;
        if(0 != queryParse.get(SAMPLE).size()){
            sample = Double.parseDouble(queryParse.get(SAMPLE).get(0));
        }

        DataFatcher df = new DataFatcher(sparkSession,queryParse.get(FROM).get(0),sample,true);

        while(confidence < lower_bound) {
            //JavaRDD<String> lines = jsc.textFile(queryParse.get(FROM).get(0), 1)
            //      .sample(true, 0.1);    //后期这里就是民叔接口的位置
            JavaRDD<String> lines = df.getNextRDDData();
            JavaRDD<List<String>> records = lines.map(new Function<String, List<String>>() {
                public List<String> call(String s) throws Exception {
                    return new ArrayList<String>(Arrays.asList(s.split("\\|")));
                }
            });
            JavaRDD<List<String>> groupbyColumn = records.map(new Function<List<String>, List<String>>() {
                public List<String> call(List<String> strings) throws Exception {
                    List<String> groupbyColumn = new ArrayList<String>();
                    for (int i = 0; i < groupbyIndex.size(); i++) {
                        groupbyColumn.add(strings.get(groupbyIndex.get(i)));
                    }
                    return groupbyColumn;
                }
            });
            JavaRDD<List<String>> groupbyReduceColumn = groupbyColumn.distinct();
            curr = groupbyReduceColumn.collect();
            confidence = GroupByUtil.computeConfidence(curr,rst);
            if(confidence <= lower_bound){
                showResult(false);
            }
            else {
                showResult(true);
            }
        }
        showAllRecord();
        return rst;
    }

    public void showResult(){
        // TODO Auto-generated method stub
       showResult(true);
    }

    public void showResult(boolean res){
        if(rst.size() <= 0){
            System.err.println("lastResult not init");
        }
        else{
            String msg;
            if (res) {
                msg = "====== SUCC ======";
            } else {
                msg = "====== NOT GOOD ======";
            }
            System.out.println(msg);
            System.out.println("   SQL:" + queryParse.get(FROM).get(0));
            System.out.println("   numbers of rows:" + rst.size());
            System.out.println("   Quantile:" + confidence);
            if (!res) {
                msg = "====== keep try ======";
            } else {
                msg = "====== END ======";
            }
            System.out.println(msg);
        }
    }

    public void showAllRecord(){
        for(int i = 0 ; i < rst.size() ; i++){
            List<String> tmp = rst.get(i);
            for(int j = 0 ; j < tmp.size() ; j++){
                System.out.print(tmp.get(j)+"      ");
            }
            System.out.println();
        }
    }
}