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

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.base.core.query.RetryPolicy;
import org.springdata.cassandra.base.core.query.RetryPolicyResolver;
import org.springdata.cassandra.base.core.query.StatementOptions;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.SimpleStatement;

/**
 * Simple query creator
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleQueryCreator implements QueryCreator {

	private final Query statement;

	public SimpleQueryCreator(String cql) {
		statement = new SimpleStatement(cql);
	}

	public SimpleQueryCreator(String cql, StatementOptions optionsOrNull) {
		statement = new SimpleStatement(cql);
		if (optionsOrNull != null) {
			if (optionsOrNull.getConsistencyLevel() != null) {
				withConsistencyLevel(optionsOrNull.getConsistencyLevel());
			}
			if (optionsOrNull.getRetryPolicy() != null) {
				withRetryPolicy(optionsOrNull.getRetryPolicy());
			}
			if (optionsOrNull.getQueryTracing() != null) {
				withQueryTracing(optionsOrNull.getQueryTracing());
			}
		}
	}

	public SimpleQueryCreator(Query query) {
		statement = query;
	}

	public SimpleQueryCreator withConsistencyLevel(ConsistencyLevel consistency) {
		if (consistency != null) {
			statement.setConsistencyLevel(ConsistencyLevelResolver.resolve(consistency));
		}
		return this;
	}

	public SimpleQueryCreator withRetryPolicy(RetryPolicy retryPolicy) {
		if (retryPolicy != null) {
			statement.setRetryPolicy(RetryPolicyResolver.resolve(retryPolicy));
		}
		return this;
	}

	public SimpleQueryCreator enableTracing() {
		statement.enableTracing();
		return this;
	}

	public SimpleQueryCreator disableTracing() {
		statement.disableTracing();
		return this;
	}

	public SimpleQueryCreator withQueryTracing(Boolean queryTracing) {
		if (queryTracing != null) {
			if (queryTracing.booleanValue()) {
				statement.enableTracing();
			} else {
				statement.disableTracing();
			}
		}
		return this;
	}

	@Override
	public Query createQuery() {
		return statement;
	}

}
