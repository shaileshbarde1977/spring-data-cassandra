/*
 * Copyright 2013 the original author or authors.
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
package net.webby.cassandrion.data.convert;

import java.nio.ByteBuffer;

import net.webby.cassandrion.data.mapping.CassandraPersistentProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * {@link PropertyValueProvider} to read property values from a {@link Row}.
 * 
 * @author Alex Shvid
 */
public class CassandraPropertyValueProvider implements PropertyValueProvider<CassandraPersistentProperty> {

	private static Logger log = LoggerFactory.getLogger(CassandraPropertyValueProvider.class);

	private final Row source;
	private final SpELExpressionEvaluator evaluator;

	/**
	 * Creates a new {@link CassandraPropertyValueProvider} with the given {@link Row} and
	 * {@link DefaultSpELExpressionEvaluator}.
	 * 
	 * @param source must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public CassandraPropertyValueProvider(Row source, DefaultSpELExpressionEvaluator evaluator) {
		Assert.notNull(source);
		Assert.notNull(evaluator);

		this.source = source;
		this.evaluator = evaluator;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getPropertyValue(CassandraPersistentProperty property) {

		String expression = property.getSpelExpression();
		if (expression != null) {
			return evaluator.evaluate(expression);
		}

		String columnName = property.getColumnName();
		if (source.isNull(property.getColumnName())) {
			return null;
		}
		DataType columnType = source.getColumnDefinitions().getType(columnName);

		log.info(columnType.getName().name());

		/*
		 * Dave Webb - Added handler for text since getBytes was throwing 
		 * InvalidTypeException when using getBytes on a text column. 
		 */
		// TODO Might need to qualify all DataTypes as we encounter them.
		if (columnType.equals(DataType.text())) {
			return (T) source.getString(columnName);
		}
		if (columnType.equals(DataType.cint())) {
			return (T) new Integer(source.getInt(columnName));
		}

		ByteBuffer bytes = source.getBytes(columnName);
		return (T) columnType.deserialize(bytes);
	}

}
