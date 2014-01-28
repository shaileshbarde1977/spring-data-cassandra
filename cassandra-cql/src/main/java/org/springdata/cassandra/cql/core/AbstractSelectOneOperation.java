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

import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Abstract implementation for SelectOneOperation
 * 
 * @author Alex Shvid
 * 
 */
public abstract class AbstractSelectOneOperation extends AbstractQueryOperation<Row, SelectOneOperation> implements
		SelectOneOperation {

	private final Query query;

	protected AbstractSelectOneOperation(CqlTemplate cqlTemplate, Query query) {
		super(cqlTemplate);
		this.query = query;
	}

	@Override
	public Row execute() {
		ResultSet resultSet = doExecute(query);
		return resultSet.one();
	}

	@Override
	public CassandraFuture<Row> executeAsync() {
		CassandraFuture<ResultSet> resultSetFuture = doExecuteAsync(query);
		ListenableFuture<Row> rowFuture = Futures.transform(resultSetFuture, new Function<ResultSet, Row>() {

			@Override
			public Row apply(ResultSet resultSet) {
				return resultSet.one();
			}

		}, getExecutor());

		CassandraFuture<Row> wrappedFuture = new CassandraFuture<Row>(rowFuture, cqlTemplate.getExceptionTranslator());
		return wrappedFuture;
	}

	@Override
	public void executeAsync(final CallbackHandler<Row> cb) {

		doExecuteAsync(query, new CallbackHandler<ResultSet>() {

			@Override
			public void onComplete(ResultSet resultSet) {
				Row row = resultSet.one();
				cb.onComplete(row);
			}

		});
	}

	@Override
	public Row executeNonstop(int timeoutMls) throws TimeoutException {
		ResultSet resultSet = doExecuteNonstop(query, timeoutMls);
		return resultSet.one();
	}

}
