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
