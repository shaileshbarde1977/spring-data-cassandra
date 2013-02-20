package org.springframework.data.cassandra.util;

import java.util.Map;

import org.springframework.data.cassandra.mapping.TableMapping;

public class CQLUtils {

	public static String generateCreateTable(TableMapping table) {

		StringBuilder str = new StringBuilder();
		str.append("CREATE TABLE ");
		str.append(table.table());
		str.append(" ( ");
		
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
		str.append(" )");
		
		return str.toString();
	}
	
}
