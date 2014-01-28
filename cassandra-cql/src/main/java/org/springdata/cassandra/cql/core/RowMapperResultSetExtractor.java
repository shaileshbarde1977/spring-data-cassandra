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
package org.springdata.cassandra.cql.core;

import java.util.Iterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * ResutSetExtractor that uses RowMapper to extract data
 * 
 * @author Alex Shvid
 * 
 */
public class RowMapperResultSetExtractor<T> implements ResultSetExtractor<Iterator<T>> {

	private final RowMapper<T> rowMapper;

	public RowMapperResultSetExtractor(RowMapper<T> rowMapper) {
		this.rowMapper = rowMapper;
	}

	@Override
	public Iterator<T> extractData(ResultSet resultSet) {
		return Iterators.transform(resultSet.iterator(), new Function<Row, T>() {

			private int rowNum = 0;

			@Override
			public T apply(Row row) {
				return rowMapper.mapRow(row, rowNum++);
			}

		});
	}
}
