package com.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Attributes wrapper for understanding the online aggregation query.
 * @author Qiao Jin
 */
public class AttributeGroup {
	// Spark online aggregation method name.
	private String operatorName;
	// The field after FROM.
	private String source;
	// The offset of Selected field in each data row.
	private String columnIndex;
	// All the conditions after WHERE, now we only support 
	private List<String> predicates = new ArrayList<String>();
	
	public void addPredicate(String predicate) {
		if (predicate == null || predicate.trim().length() == 0) {
			return;
		}

		this.predicates.add(predicate);
	}
	
	/** Getters and Setters. */
	public String getOperatorName() {
		return operatorName;
	}
	
	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}
	
	public String getSource() {
		return source;
	}
	
	public void setSource(String source) {
		this.source = source;
	}
	
	public String getColumnIndex() {
		return columnIndex;
	}
	
	public void setColumnIndex(String columnIndex) {
		this.columnIndex = columnIndex;
	}
	
	public List<String> getPredicates() {
		return predicates;
	}
	
	public void setPredicates(List<String> predicates) {
		this.predicates = predicates;
	}
	
	@Override
	public String toString() {
		return String.format("{operatorName: %s; source: %s;  columnIndex: %s; predicates: %s}", 
				operatorName, source, columnIndex, predicates);
	}
}