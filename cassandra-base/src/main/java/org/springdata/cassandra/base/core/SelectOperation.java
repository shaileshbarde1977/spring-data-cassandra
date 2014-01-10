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
 * 
 * @author Alex Shvid
 * 
 */

public interface SelectOperation<T> {

	SelectOperation<T> withConsistencyLevel(ConsistencyLevel consistencyLevel);

	SelectOperation<T> withRetryPolicy(RetryPolicy retryPolicy);

	SelectOperation<T> withQueryTracing(Boolean queryTracing);

	<R> SimpleSelectOperation<Iterator<R>> map(RowMapper<R> rowMapper);

	<R> SimpleSelectOperation<R> mapOne(RowMapper<R> rowMapper);

	<E> SimpleSelectOperation<E> firstColumnOne(Class<E> elementType);

	<E> SimpleSelectOperation<Iterator<E>> firstColumn(Class<E> elementType);

	SimpleSelectOperation<Iterator<Map<String, Object>>> map();

	SimpleSelectOperation<Map<String, Object>> mapOne();

	<O> SimpleSelectOperation<O> transform(ResultSetCallback<O> rsc);

	SimpleSelectOperation<Object> each(RowCallbackHandler rch);

	SelectOperation<T> withFallbackHandler(FallbackHandler fh);

	SelectOperation<T> withExecutor(Executor executor);

	T execute();

	CassandraFuture<T> executeAsync();

	T executeNonstop(int timeoutMls) throws TimeoutException;

}
