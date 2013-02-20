/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.StringUtils;

/**
 * Cassandra specific {@link org.springframework.data.mapping.model.AnnotationBasedPersistentProperty} implementation.
 * 
 * @author Alex Shvid
 */
public class BasicCassandraPersistentProperty extends AnnotationBasedPersistentProperty<CassandraPersistentProperty> implements
CassandraPersistentProperty {
	
	/**
	 * Creates a new {@link BasicCassandraPersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 */
	public BasicCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
	}
	
	/**
	 * Also considers fields that has a RowId annotation.
	 * 
	 */
	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		return getField().isAnnotationPresent(RowId.class);
	}
	
	/**
	 * For dynamic tables returns true if property value is used as column name. 
	 * 
	 * @return
	 */
	public boolean isColumnId() {
		return getField().isAnnotationPresent(ColumnId.class);
	}
	
	/**
	 * Returns the column name to be used to store the value of the property inside the Cassandra.
	 * 
	 * @return
	 */
	public String getColumnName() {
		Column annotation = getField().getAnnotation(Column.class);
		return annotation != null && StringUtils.hasText(annotation.value()) ? annotation.value() : field.getName();
	}
	
	/**
	 * Returns true if the property has secondary index on this column. 
	 * 
	 * @return
	 */
	public boolean isIndexed() {
		return getField().isAnnotationPresent(Index.class);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<CassandraPersistentProperty>(this, null);
	}
	
}
