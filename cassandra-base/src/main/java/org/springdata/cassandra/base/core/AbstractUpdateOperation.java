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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.base.core.query.RetryPolicy;
import org.springdata.cassandra.base.core.query.RetryPolicyResolver;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

/**
 * 
 * @author Alex Shvid
 * 
 */

public abstract class AbstractUpdateOperation implements UpdateOperation {

	private final CassandraTemplate cassandraTemplate;

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;

	public AbstractUpdateOperation(CassandraTemplate cassandraTemplate) {
		this.cassandraTemplate = cassandraTemplate;
	}

	abstract Query getQuery();

	@Override
	public UpdateOperation withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public UpdateOperation withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	@Override
	public UpdateOperation withQueryTracing(Boolean queryTracing) {
		this.queryTracing = queryTracing;
		return this;
	}

	@Override
	public ResultSet execute() {
		Query query = getQuery();
		addQueryOptions(query);
		return cassandraTemplate.doExecute(query);
	}

	@Override
	public ResultSetFuture executeAsync() {
		Query query = getQuery();
		addQueryOptions(query);
		return cassandraTemplate.doExecuteAsync(query);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		Query query = getQuery();
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraTemplate.doExecuteAsync(query);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
				cassandraTemplate.getExceptionTranslator());
		return wrappedFuture.getUninterruptibly(timeoutMls, TimeUnit.MILLISECONDS);
	}

	private void addQueryOptions(Query query) {

		/*
		 * Add Query Options
		 */

		if (consistencyLevel != null) {
			query.setConsistencyLevel(ConsistencyLevelResolver.resolve(consistencyLevel));
		}

		if (retryPolicy != null) {
			query.setRetryPolicy(RetryPolicyResolver.resolve(retryPolicy));
		}

		if (queryTracing != null) {
			if (queryTracing.booleanValue()) {
				query.enableTracing();
			} else {
				query.disableTracing();
			}
		}
	}

}
