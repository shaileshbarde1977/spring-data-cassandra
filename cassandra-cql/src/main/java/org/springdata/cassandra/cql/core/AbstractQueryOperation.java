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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.cql.core.query.ConsistencyLevel;
import org.springdata.cassandra.cql.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.cql.core.query.RetryPolicy;
import org.springdata.cassandra.cql.core.query.RetryPolicyResolver;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 
 * @author Alex Shvid
 * 
 * @param <T>
 * @param <O>
 */

public abstract class AbstractQueryOperation<T, O extends QueryOperation<T, O>> implements QueryOperation<T, O> {

	protected final CassandraCqlTemplate cassandraCqlTemplate;

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;

	private FallbackHandler fh;
	private Executor executor;

	protected AbstractQueryOperation(CassandraCqlTemplate cassandraCqlTemplate) {
		Assert.notNull(cassandraCqlTemplate);
		this.cassandraCqlTemplate = cassandraCqlTemplate;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		Assert.notNull(consistencyLevel);
		this.consistencyLevel = consistencyLevel;
		return (O) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O withRetryPolicy(RetryPolicy retryPolicy) {
		Assert.notNull(retryPolicy);
		this.retryPolicy = retryPolicy;
		return (O) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O withQueryTracing(Boolean queryTracing) {
		Assert.notNull(queryTracing);
		return (O) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O withFallbackHandler(FallbackHandler fh) {
		Assert.notNull(fh);
		this.fh = fh;
		return (O) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O withExecutor(Executor executor) {
		Assert.notNull(executor);
		this.executor = executor;
		return (O) this;
	}

	protected void addQueryOptions(Query query) {

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

	protected Query doCreateQuery(QueryCreator qc) {
		return cassandraCqlTemplate.createQuery(qc);
	}

	protected ResultSet doExecute(Query query) {
		addQueryOptions(query);
		return cassandraCqlTemplate.doExecute(query);
	}

	protected CassandraFuture<ResultSet> doExecuteAsync(Query query) {
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraCqlTemplate.doExecuteAsync(query);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
				cassandraCqlTemplate.getExceptionTranslator());
		return wrappedFuture;
	}

	protected void doExecuteAsync(Query query, final CallbackHandler<ResultSet> cb) {
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraCqlTemplate.doExecuteAsync(query);
		doFutureCallback(resultSetFuture, cb);
	}

	protected <R> void doFutureCallback(ListenableFuture<R> future, final CallbackHandler<R> cb) {

		Futures.addCallback(future, new FutureCallback<R>() {

			@Override
			public void onSuccess(R result) {
				cb.onComplete(result);
			}

			@Override
			public void onFailure(Throwable t) {
				if (t instanceof RuntimeException) {
					t = cassandraCqlTemplate.translateIfPossible((RuntimeException) t);
				}
				fireOnFailure(t);
			}

		}, getExecutor());
	}

	protected ResultSet doExecuteNonstop(Query query, int timeoutMls) throws TimeoutException {
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraCqlTemplate.doExecuteAsync(query);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
				cassandraCqlTemplate.getExceptionTranslator());
		return wrappedFuture.getUninterruptibly(timeoutMls, TimeUnit.MILLISECONDS);
	}

	protected Executor getExecutor() {
		return executor != null ? executor : MoreExecutors.sameThreadExecutor();
	}

	protected void fireOnFailure(Throwable t) {
		if (fh != null) {
			fh.onFailure(t);
		}
	}

}
