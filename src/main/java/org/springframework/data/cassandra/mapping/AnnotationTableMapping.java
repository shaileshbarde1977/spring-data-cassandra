package org.springframework.data.cassandra.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.util.StringUtils;

public class AnnotationTableMapping implements TableMapping {

	private Class<?> entityClass;
	private String table;
	private String rowId;
	private String columnId;
	private List<String> indexFields = new ArrayList<String>();
	private Map<String, String> fields = new HashMap<String, String>();
	
	public AnnotationTableMapping(String entity) {
		try {
			entityClass = Class.forName(entity);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("invalid entity class " + entity, e);
		}
		
		for (Annotation annotation : entityClass.getDeclaredAnnotations()) {
			if (annotation instanceof Table) {
				table = ((Table) annotation).name();
				break;
			}
		}
		
		if (!StringUtils.hasText(table)) {
			table = entityClass.getSimpleName();
		}
		
	    for (Field field : entityClass.getDeclaredFields()) {
	    	for (Annotation annotation : field.getDeclaredAnnotations()) {
	    		if (annotation instanceof RowId || annotation instanceof Id) {
	    			rowId = field.getName();
	    		}
	    		else if (annotation instanceof ColumnId) {
	    			columnId = field.getName();
	    		}
	    		else if (annotation instanceof Index) {
	    			indexFields.add(field.getName());
	    		}
	    	}
	    	fields.put(field.getName(), getCassandraType(field.getType()));
	    }
		
	}
	
	public String table() {
		return table;
	}

	public String rowId() {
		return rowId;
	}

	public String columnId() {
		return columnId;
	}

	public String[] indexFields() {
		return indexFields.toArray(new String[indexFields.size()]);
	}

	public Map<String, String> fields() {
		return fields;
	}
	
	private static String getCassandraType(Class<?> type) {
		return "text";
	}
	
}
