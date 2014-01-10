package org.springdata.cassandra.base.core;

import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.RetryPolicy;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

public interface UpdateOperation {

	UpdateOperation withConsistencyLevel(ConsistencyLevel consistencyLevel);

	UpdateOperation withRetryPolicy(RetryPolicy retryPolicy);

	UpdateOperation withQueryTracing(Boolean queryTracing);

	ResultSet execute();

	ResultSetFuture executeAsync();

	ResultSet executeNonstop(int timeoutMls) throws TimeoutException;

}
