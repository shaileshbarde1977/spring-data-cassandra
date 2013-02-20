package org.springframework.data.cassandra.mapping;

import java.util.Map;

public interface TableMapping {

	public String table();
	
	public String rowId();
	
	public String columnId();
	
	public String[] indexFields();
	
	public Map<String, String> fields();
}
