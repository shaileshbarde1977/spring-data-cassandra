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

import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.RetryPolicy;

/**
 * Base methods for Cassandra select operation
 * 
 * @author Alex Shvid
 * 
 */

public interface BaseSelectOperation<T> {

	/**
	 * Adds consistency level to the select operation
	 * 
	 * @param consistencyLevel ConsistencyLevel
	 * @return this
	 */
	BaseSelectOperation<T> withConsistencyLevel(ConsistencyLevel consistencyLevel);

	/**
	 * Adds retry policy to the select operation
	 * 
	 * @param retryPolicy RetryPolicy
	 * @return this
	 */
	BaseSelectOperation<T> withRetryPolicy(RetryPolicy retryPolicy);

	/**
	 * Adds query tracing option to the select operation
	 * 
	 * @param queryTracing Boolean to enable/disable query tracing
	 * @return this
	 */
	BaseSelectOperation<T> withQueryTracing(Boolean queryTracing);

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
