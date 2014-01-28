/*
 * Copyright 2014 the original author or authors.
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
package org.springdata.cassandra.core;

import java.util.Iterator;

import org.springdata.cassandra.cql.core.ResultSetExtractor;
import org.springframework.data.convert.EntityReader;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Extracts data from the ResultSet by using Cassandra Converter for particular Entity
 * 
 * @author Alex Shvid
 * 
 */
public class ReaderResultSetExtractor<T> implements ResultSetExtractor<Iterator<T>> {

	private final EntityReader<? super T, Object> reader;
	private final Class<T> entityClass;

	public ReaderResultSetExtractor(EntityReader<? super T, Object> reader, Class<T> entityClass) {
		Assert.notNull(reader);
		Assert.notNull(entityClass);
		this.reader = reader;
		this.entityClass = entityClass;
	}

	@Override
	public Iterator<T> extractData(ResultSet resultSet) {

		return Iterators.transform(resultSet.iterator(), new Function<Row, T>() {

			@Override
			public T apply(Row row) {
				T source = reader.read(entityClass, row);
				return source;
			}

		});

	}

}
