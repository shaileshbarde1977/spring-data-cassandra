package org.springdata.cassandra.base.core;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.base.core.query.RetryPolicy;
import org.springdata.cassandra.base.core.query.RetryPolicyResolver;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class AbstractQueryOperation<T, O extends QueryOperation<T, O>> implements QueryOperation<T, O> {

	protected final CassandraTemplate cassandraTemplate;

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;

	protected FallbackHandler fh;
	protected Executor executor;

	protected AbstractQueryOperation(CassandraTemplate cassandraTemplate) {
		this.cassandraTemplate = cassandraTemplate;
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

	protected ResultSet doExecute(Query query) {
		addQueryOptions(query);
		return cassandraTemplate.doExecute(query);
	}

	protected CassandraFuture<ResultSet> doExecuteAsync(Query query) {
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraTemplate.doExecuteAsync(query);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
				cassandraTemplate.getExceptionTranslator());
		return wrappedFuture;
	}

	protected void doExecuteAsync(Query query, final CallbackHandler<ResultSet> cb) {
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraTemplate.doExecuteAsync(query);
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
				if (fh != null) {
					if (t instanceof RuntimeException) {
						t = cassandraTemplate.translateIfPossible((RuntimeException) t);
					}
					fh.onFailure(t);
				}
			}

		}, executor != null ? executor : MoreExecutors.sameThreadExecutor());
	}

	protected ResultSet doExecuteNonstop(Query query, int timeoutMls) throws TimeoutException {
		addQueryOptions(query);
		ResultSetFuture resultSetFuture = cassandraTemplate.doExecuteAsync(query);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture,
				cassandraTemplate.getExceptionTranslator());
		return wrappedFuture.getUninterruptibly(timeoutMls, TimeUnit.MILLISECONDS);
	}

}
