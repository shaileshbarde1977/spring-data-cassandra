package org.springdata.cassandra.base.core;

import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.RetryPolicy;

public interface IngestOperation {

	IngestOperation withConsistencyLevel(ConsistencyLevel consistencyLevel);

	IngestOperation withRetryPolicy(RetryPolicy retryPolicy);

	IngestOperation withQueryTracing(Boolean queryTracing);

	void execute();

	void executeAsync();

	void executeNonstop(int timeoutMls) throws TimeoutException;

}
