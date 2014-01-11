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

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;

/**
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultUpdateOperation extends AbstractQueryOperation<ResultSet, UpdateOperation> implements
		UpdateOperation {

	private final Query query;

	protected DefaultUpdateOperation(CassandraTemplate cassandraTemplate, String cql) {
		this(cassandraTemplate, new SimpleStatement(cql));
	}

	protected DefaultUpdateOperation(CassandraTemplate cassandraTemplate, Query query) {
		super(cassandraTemplate);
		this.query = query;
	}

	@Override
	public ResultSet execute() {
		return doExecute(query);
	}

	@Override
	public CassandraFuture<ResultSet> executeAsync() {
		return doExecuteAsync(query);
	}

	@Override
	public void executeAsync(final CallbackHandler<ResultSet> cb) {
		doExecuteAsync(query, cb);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		return doExecuteNonstop(query, timeoutMls);
	}

}
