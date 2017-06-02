package com.utils;

import java.util.logging.Logger;

/**
 * Parser engine to understand the semantic of input query, support embedded query with multiple keywords.
 * @author Qiao Jin
 */
public class QueryParser {
	private static final Logger logger = Logger.getLogger(QueryParser.class.getName());
	
	// Built-in semantic keywords for online aggregation query.
	private final static String SELECT = "select";
	private final static String FROM = "from";
	private final static String WHERE = "where";
	private final static String AND = "and";
	// TODO implement in phase 2.
	private final static String OR = "or";
	private final static String GROUP_BY = "group by";

	/**
	 * Parse the query and extract all the attributes, the format must be
	 * "select xxx from xxx where xxx and xxx or xxx"
	 */
	public AttributeGroup parse(String query) {
		if (query == null || query.trim().length() == 0) {
			logger.warning("Input query is invalid!");
			return null;
		}
		
		String[] tokens = split(query);
		if (tokens == null || tokens.length == 0) {
			return null;
		}
		
		AttributeGroup group = new AttributeGroup();
		boolean isBuiltInKeyWord = false;
		String previousKeyword = null;
		
		for (String token : tokens) {
			String lowerCasedToken = token.toLowerCase();
			if (lowerCasedToken.equals(SELECT) || lowerCasedToken.equals(FROM) ||
				lowerCasedToken.equals(WHERE) || lowerCasedToken.equals(AND) ||
				lowerCasedToken.equals(OR) || lowerCasedToken.equals(GROUP_BY)) {
				if (isBuiltInKeyWord) {  // Encounter consecutive built-in keywords, mark it as invalid query.
					logger.warning("No consecutive built-in keywords ALLOWED in the query!");
					return null;
				}
				isBuiltInKeyWord = true;
				previousKeyword = lowerCasedToken;
			} else { // Fields.
				if (!isBuiltInKeyWord) {
					logger.warning("No consecutive fields ALLOWED in the query, now we limit the format should be"
							+ "select func(Rx) from xxx where xxx (and xxx)* (or xxx)* group by xxx");
					return null;
				}
				// Check with different conditions.
				if (previousKeyword.equals(SELECT)) { // Selected field.
					String operatorName = token;
					int leftParethesis = token.indexOf("("),
						rightParethesis = token.indexOf(")");
					
					if (leftParethesis != -1 && rightParethesis != -1) {  // Extract column index, the format is Rx.
						String columnIndex = token.substring(leftParethesis + 1, rightParethesis);

						group.setColumnIndex(columnIndex.startsWith("R")
								? columnIndex.substring(1)
								: columnIndex);
						operatorName = token.substring(0, leftParethesis);
					}
					group.setOperatorName(operatorName);
				} else if (previousKeyword.equals(FROM)) {  // The data source.
					group.setSource(token);
				} else if (previousKeyword.equals(WHERE) || previousKeyword.equals(AND)) {  // clauses.
					group.addPredicate(token);
				} else if (previousKeyword.equals(OR) || previousKeyword.equals(GROUP_BY)) {  // Implement it in phase 2.
					// no-op.
				}
				// Flip the status.
				isBuiltInKeyWord = false;
			}
		}
		
		if (isBuiltInKeyWord) {
			logger.warning("built-in keywords left without associated column or table names, the query is invalid!");
			return null;
		}
		
		return group;
	}
	
	public String[] split(String query) {
		return query.split(" +|\n");
	}

	// test.
	public static void main(String[] args) {
		QueryParser parser = new QueryParser();

		String query = "Select sum(R2) fROM    s  Where a==b ANd e==f";
		AttributeGroup group = parser.parse(query);
		System.out.println(group);
	}
}