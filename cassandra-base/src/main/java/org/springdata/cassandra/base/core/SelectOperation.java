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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.RetryPolicy;

/**
 * General class for select operations. Support transformation and mappring of the ResultSet
 * 
 * @author Alex Shvid
 * 
 */

public interface SelectOperation<T> {

	/**
	 * Adds consistency level to the select operation.
	 * 
	 * @param consistencyLevel ConsistencyLevel
	 * @return this
	 */
	SelectOperation<T> withConsistencyLevel(ConsistencyLevel consistencyLevel);

	/**
	 * Adds retry policy to the select operation
	 * 
	 * @param retryPolicy RetryPolicy
	 * @return this
	 */
	SelectOperation<T> withRetryPolicy(RetryPolicy retryPolicy);

	/**
	 * Adds query tracing option to the select operation.
	 * 
	 * @param queryTracing Boolean to enable/disable query tracing
	 * @return this
	 */
	SelectOperation<T> withQueryTracing(Boolean queryTracing);

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

	/**
	 * Uses fallback handler to send errors in asynchronous execution.
	 * 
	 * @param fh
	 * @return this
	 */
	SelectOperation<T> withFallbackHandler(FallbackHandler fh);

	/**
	 * Specifies Executor that will be used for asynchronous execution. By default will be used SameThreadExecutor that
	 * will be the thread that calls ResultSetFuture.set() and it will be Datastax Driver internal thread. It is
	 * recommended to specify application thread pool executor for asynchronous calls.
	 * 
	 * @param executor Executor service
	 * @return this
	 */
	SelectOperation<T> withExecutor(Executor executor);

	/**
	 * Synchronously executes select operation and returns object with type T
	 * 
	 * @return object with type T
	 */
	T execute();

	/**
	 * Asynchronously executes select operation and returns Future of the object with type T
	 * 
	 * @return Future of the object with type T
	 */
	CassandraFuture<T> executeAsync();

	/**
	 * Synchronously executes select operation for the given time interval in milliseconds and returns object with type T.
	 * Useful for SLA services that guarantees response time.
	 * 
	 * @return object with type T or throws TimeoutException if execution time is grater than expected. Actually does not
	 *         cancel operation on Cassandra servers due to unsupported feature in Cassandra itself.
	 * 
	 */
	T executeNonstop(int timeoutMls) throws TimeoutException;

}
