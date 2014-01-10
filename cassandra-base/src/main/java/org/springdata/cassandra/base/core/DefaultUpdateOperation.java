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
import com.datastax.driver.core.SimpleStatement;

/**
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultUpdateOperation implements UpdateOperation {

	private final CassandraTemplate cassandraTemplate;
	private final Query query;

	protected DefaultUpdateOperation(CassandraTemplate cassandraTemplate, String cql) {
		this(cassandraTemplate, new SimpleStatement(cql));
	}

	protected DefaultUpdateOperation(CassandraTemplate cassandraTemplate, Query query) {
		this.cassandraTemplate = cassandraTemplate;
		this.query = query;
	}

	@Override
	public UpdateOperation withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		if (consistencyLevel != null) {
			query.setConsistencyLevel(ConsistencyLevelResolver.resolve(consistencyLevel));
		}
		return this;
	}

	@Override
	public UpdateOperation withRetryPolicy(RetryPolicy retryPolicy) {
		if (retryPolicy != null) {
			query.setRetryPolicy(RetryPolicyResolver.resolve(retryPolicy));
		}
		return this;
	}

	@Override
	public UpdateOperation withQueryTracing(Boolean queryTracing) {
		if (queryTracing != null) {
			if (queryTracing.booleanValue()) {
				query.enableTracing();
			} else {
				query.disableTracing();
			}
		}
		return this;
	}

	@Override
	public ResultSet execute() {
		return cassandraTemplate.doExecute(query);
	}

	@Override
	public ResultSetFuture executeAsync() {
		return cassandraTemplate.doExecuteAsync(query);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		ResultSetFuture resultSetFuture = cassandraTemplate.doExecuteAsync(query);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
				cassandraTemplate.getExceptionTranslator());
		return wrappedFuture.getUninterruptibly(timeoutMls, TimeUnit.MILLISECONDS);
	}

}
