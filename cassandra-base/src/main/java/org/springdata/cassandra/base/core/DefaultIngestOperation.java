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

public class DefaultIngestOperation implements IngestOperation {

	private final CassandraTemplate cassandraTemplate;
	private final Iterator<Query> queryIterator;

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;

	public DefaultIngestOperation(CassandraTemplate cassandraTemplate, Iterator<Query> iterator) {
		this.cassandraTemplate = cassandraTemplate;
		this.queryIterator = iterator;
	}

	@Override
	public IngestOperation withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	@Override
	public IngestOperation withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	@Override
	public IngestOperation withQueryTracing(Boolean queryTracing) {
		this.queryTracing = queryTracing;
		return this;
	}

	@Override
	public void execute() {

		while (queryIterator.hasNext()) {
			Query query = queryIterator.next();
			addQueryOptions(query);
			cassandraTemplate.doExecute(query);
		}

	}

	@Override
	public void executeAsync() {

		while (queryIterator.hasNext()) {
			Query query = queryIterator.next();
			addQueryOptions(query);
			cassandraTemplate.doExecuteAsync(query);
		}

	}

	@Override
	public void executeNonstop(int timeoutMls) throws TimeoutException {

		while (queryIterator.hasNext()) {
			Query query = queryIterator.next();
			addQueryOptions(query);
			ResultSetFuture resultSetFuture = cassandraTemplate.doExecuteAsync(query);
			CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
					cassandraTemplate.getExceptionTranslator());
			wrappedFuture.getUninterruptibly(timeoutMls, TimeUnit.MILLISECONDS);
		}

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
