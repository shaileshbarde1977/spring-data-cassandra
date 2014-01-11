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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

/**
 * Base interface that describes methods that can be used for Cassandra update operation.
 * 
 * @author Alex Shvid
 * 
 */

public interface CopyOfUpdateOperation {

	/**
	 * Adds consistency level to the update operation
	 * 
	 * @param consistencyLevel ConsistencyLevel
	 * @return this
	 */
	CopyOfUpdateOperation withConsistencyLevel(ConsistencyLevel consistencyLevel);

	/**
	 * Adds retry policy to the update operation
	 * 
	 * @param retryPolicy RetryPolicy
	 * @return this
	 */
	CopyOfUpdateOperation withRetryPolicy(RetryPolicy retryPolicy);

	/**
	 * Adds query tracing option to the update operation
	 * 
	 * @param queryTracing Boolean to enable/disable query tracing
	 * @return this
	 */
	CopyOfUpdateOperation withQueryTracing(Boolean queryTracing);

	/**
	 * Synchronously executes update operation and returns empty ResultSet
	 * 
	 * @return ResultSet
	 */
	ResultSet execute();

	/**
	 * Asynchronously executes update operation and returns Future of the ResultSet
	 * 
	 * @return ResultSetFuture
	 */
	ResultSetFuture executeAsync();

	/**
	 * Asynchronously executes update operation and call CallbackHandler with result on completion
	 * 
	 * @return ResultSetFuture
	 */
	void executeAsync(CallbackHandler<ResultSet> cb);

	/**
	 * Synchronously executes update operation for the given time interval in milliseconds and returns empty ResultSet.
	 * Useful for SLA services that guarantees response time.
	 * 
	 * @return ResultSet or throws TimeoutException if execution time is grater than expected. Actually does not cancel
	 *         operation on Cassandra servers due to unsupported feature in Cassandra itself. Operation can be
	 *         completed/uncompleted on servers.
	 * 
	 */
	ResultSet executeNonstop(int timeoutMls) throws TimeoutException;

}
