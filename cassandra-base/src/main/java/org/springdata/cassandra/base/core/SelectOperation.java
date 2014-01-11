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
package org.springdata.cassandra.base.core;

import java.util.Iterator;
import java.util.Map;

/**
 * General class for select operations. Support transformation and mapping of the ResultSet
 * 
 * @author Alex Shvid
 * 
 */

public interface SelectOperation<T> extends QueryOperation<T, SelectOperation<T>> {

	/**
	 * Maps each row in ResultSet by RowMapper.
	 * 
	 * @param rowMapper
	 * @return new mapped select operation
	 */
	<R> BaseSelectOperation<Iterator<R>> map(RowMapper<R> rowMapper);

	/**
	 * Maps first row in ResultSet by RowMapper.
	 * 
	 * @param rowMapper
	 * @return new mapped select operation
	 */
	<R> BaseSelectOperation<R> mapOne(RowMapper<R> rowMapper);

	/**
	 * Retrieves first row in the first column, expected type is elementType class.
	 * 
	 * @param elementType
	 * @return new mapped select operation
	 */
	<E> BaseSelectOperation<E> firstColumnOne(Class<E> elementType);

	/**
	 * Retrieves only the first column from ResultSet, expected type is elementType class.
	 * 
	 * @param elementType
	 * @return new mapped select operation
	 */
	<E> BaseSelectOperation<Iterator<E>> firstColumn(Class<E> elementType);

	/**
	 * Maps all rows from ResultSet to Map<String, Object>.
	 * 
	 * @return new mapped select operation
	 */
	BaseSelectOperation<Iterator<Map<String, Object>>> map();

	/**
	 * Maps only first row from ResultSet to Map<String, Object>.
	 * 
	 * @return new mapped select operation
	 */
	BaseSelectOperation<Map<String, Object>> mapOne();

	/**
	 * Uses ResultSetCallback to transform ResultSet to object with type T.
	 * 
	 * @param rsc
	 * @return new mapped select operation
	 */
	<O> BaseSelectOperation<O> transform(ResultSetCallback<O> rsc);

	/**
	 * Calls RowCallbackHandler for each row in ResultSet.
	 * 
	 * @param rch
	 * @return new mapped select operation
	 */
	BaseSelectOperation<Object> each(RowCallbackHandler rch);

}
