package com.funcs;

import org.apache.spark.api.java.JavaRDD;

import com.utils.AttributeGroup;
import com.utils.QueryParser;

/**
 * Count operation for spark online aggregation.
 * @author Qiao Jin
 */
public class CountOperator implements OnlineAggregationOperation{
	
	private Long lastResult;
	
	private QueryParser parser = new QueryParser();
	
	private JavaRDD<String> dataSource;
	
	public CountOperator(JavaRDD<String> dataSource) {
		this.dataSource = dataSource;
	}
	
	@Override
	public Object exec(String query) {
		AttributeGroup group = parser.parse(query);
		if (group == null) {
			return null;
		}

		lastResult = Long.valueOf(dataSource.count());
		return lastResult;
	}

	@Override
	public void showResult() {
		if (lastResult == null) {
    		System.out.println("No COUNT operation executed, result is unavailable!");
    	} else {
    		System.out.println(String.format("The current COUNT is: %d", lastResult.longValue())); 
    	}
	}

}
