package org.springframework.data.cassandra.util;


public class CQLUtils {

	public static String generateCreateTable(String tableName) {

		StringBuilder str = new StringBuilder();
		str.append("CREATE TABLE ");
		str.append(tableName);
		str.append(" ( ");

		/*
		boolean first = true;
		for (Map.Entry<String, String> entry : table.fields().entrySet()) {
			if (!first) {
				str.append(", ");
			}
			first = false;
			str.append(entry.getKey());
			str.append(" ");
			str.append(entry.getValue());
		}
		*/
		str.append(" )");
		
		
		return str.toString();
	}
	
}
