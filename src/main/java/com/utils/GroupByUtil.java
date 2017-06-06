package com.utils;


import java.util.*;
import java.util.logging.Logger;

import static com.utils.Constants.*;

/**
 * Created by xupeng on 2017/6/3.
 */
public class GroupByUtil {

    static Logger logger = Logger.getLogger(GroupByUtil.class.getName());

    public static int max = 0;
    public static int nums = 0;
    public static Set<Integer> pool = new HashSet<Integer>();

    public static Map<String,List<String>> parseQuery(String query){

//        final String SELECT = "select";
//        final String FROM = "from";
//        final String GROUPBY = "group by";

        Map<String,List<String>> result = new HashMap<String, List<String>>();

        String queryLowerCase = query.toLowerCase();

        if(!queryLowerCase.contains(SELECT)){
            logger.info("There is no \"" + SELECT + "\" in the query!");
            System.out.println("There is no \"" + SELECT + "\" in the query!");
            return null;
        }

        if(!queryLowerCase.contains(FROM)){
            logger.info("There is no \"" + FROM + "\" in the query!");
            System.out.println("There is no \"" + FROM + "\" in the query!");
            return null;
        }

        if(!queryLowerCase.contains(GROUPBY)){
            logger.info("There is no \"" + GROUPBY + "\" in the query!");
            System.out.println("There is no \"" + GROUPBY + "\" in the query!");
            return null;
        }

        String selectString = query.substring(queryLowerCase.indexOf(SELECT)+SELECT.length(),
                queryLowerCase.indexOf(FROM)).trim();
        String url = query.substring(queryLowerCase.indexOf(FROM)+FROM.length(),
                queryLowerCase.indexOf(GROUPBY)).trim();
        String groupbyString = "";
        String sampleString = "";
        String confidenceString = "";
        if(queryLowerCase.contains(SAMPLE)){
            groupbyString = query.substring(queryLowerCase.indexOf(GROUPBY)+GROUPBY.length(),
                    queryLowerCase.indexOf(SAMPLE)).trim();
            if(queryLowerCase.contains(CONFIDENCE)){
                sampleString = query.substring(queryLowerCase.indexOf(SAMPLE)+SAMPLE.length(),
                        queryLowerCase.indexOf(CONFIDENCE)).trim();
                confidenceString = query.substring(queryLowerCase.indexOf(CONFIDENCE)+CONFIDENCE.length()).trim();
            }
            else{
                sampleString = query.substring(queryLowerCase.indexOf(SAMPLE)+SAMPLE.length()).trim();
            }
        }
        else if(queryLowerCase.contains(CONFIDENCE)){
            groupbyString = query.substring(queryLowerCase.indexOf(GROUPBY) + GROUPBY.length(),
                    queryLowerCase.indexOf(CONFIDENCE)).trim();
            confidenceString = query.substring(queryLowerCase.indexOf(CONFIDENCE) + CONFIDENCE.length()).trim();
        }
        else{
            groupbyString = query.substring(queryLowerCase.indexOf(GROUPBY) + GROUPBY.length()).trim();
        }

        List<String> selectList = new ArrayList<String>(Arrays.asList(selectString.split(",")));
        result.put(SELECT,selectList);
        List<String> from = new ArrayList<String>();
        from.add(url.substring(url.indexOf("<")+1,url.indexOf(">")));
        result.put(FROM,from);
        List<String> groupbyList = new ArrayList<String>(Arrays.asList(groupbyString.split(",")));
        result.put(GROUPBY,groupbyList);
        List<String> sample = new ArrayList<String>();
        sample.add(sampleString);
        result.put(SAMPLE,sample);
        List<String> confidence = new ArrayList<String>();
        confidence.add(confidenceString);
        result.put(CONFIDENCE,confidence);

        return result;
    }

    public static List<Integer> getLineIndex(List<String> list){
        if(null == list || 0 == list.size()){
            logger.info("The form of index is illegal!");
            System.out.println("The form of index is illegal!");
            return null;
        }
        List<Integer> result = new ArrayList<Integer>();
        for(int i = 0 ; i < list.size() ; i++){
            String tmp = list.get(i).trim();
            if(tmp.contains("R")){
                String index = tmp.substring(tmp.indexOf("R")+1).trim();
                result.add(Integer.parseInt(index));
            }
            else{
                logger.info("\"The form of index is illegal!\"");
                System.out.println("\"The form of index is illegal!\"");
                return null;
            }
        }
        return result;
    }

    public static double computeConfidence(List<List<String>> list,List<List<String>> result){
        for(int i = 0 ; i < list.size() ; i++){
            int tmp = list.get(i).hashCode();
            int p = Integer.numberOfTrailingZeros(tmp);
            if(!pool.contains(tmp)){
                pool.add(tmp);
                result.add(list.get(i));
                nums++;
            }
            if(p > max){
                max = p;
            }
        }
        return 1.0 - Math.exp(-1 * nums * Math.pow(2,max * -1));
    }
}