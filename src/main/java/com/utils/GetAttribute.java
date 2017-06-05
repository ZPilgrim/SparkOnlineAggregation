package com.utils;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by chengrui on 2017/5/21.
 */
public class GetAttribute implements Serializable, FlatMapFunction<String, String>{
    int index_of_attribute;
    public GetAttribute(int index){
        index_of_attribute = index;
    }
    @Override
    public Iterator<String> call(String s) throws Exception {
        String value = s.split("\\|")[index_of_attribute];
        String[] values = new String[1];
        values[0] = value;
        ArrayList<String> arrList = new ArrayList<String>();
        arrList.add(value);
        Iterator<String> itr = arrList.iterator();
        return Arrays.asList(values).iterator();
    }
}
