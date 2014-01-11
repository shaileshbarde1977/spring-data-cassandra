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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultIngestOperation extends AbstractQueryOperation<List<ResultSet>, IngestOperation> implements
		IngestOperation {

	private final Iterator<Query> queryIterator;

	public DefaultIngestOperation(CassandraTemplate cassandraTemplate, Iterator<Query> iterator) {
		super(cassandraTemplate);
		this.queryIterator = iterator;
	}

	@Override
	public List<ResultSet> execute() {

		List<ResultSet> list = new ArrayList<ResultSet>();

		while (queryIterator.hasNext()) {
			Query query = queryIterator.next();
			ResultSet resultSet = doExecute(query);
			list.add(resultSet);
		}

		return list;

	}

	@Override
	public CassandraFuture<List<ResultSet>> executeAsync() {

		final Iterator<ListenableFuture<ResultSet>> resultSetFutures = Iterators.transform(queryIterator,
				new Function<Query, ListenableFuture<ResultSet>>() {

					@Override
					public ListenableFuture<ResultSet> apply(Query query) {
						return doExecuteAsync(query);
					}

				});

		ListenableFuture<List<ResultSet>> allResultSetFuture = Futures
				.successfulAsList(new Iterable<ListenableFuture<ResultSet>>() {

					@Override
					public Iterator<ListenableFuture<ResultSet>> iterator() {
						return resultSetFutures;
					}

				});

		CassandraFuture<List<ResultSet>> wrappedFuture = new CassandraFuture<List<ResultSet>>(allResultSetFuture,
				cassandraTemplate.getExceptionTranslator());

		return wrappedFuture;
	}

	@Override
	public void executeAsync(CallbackHandler<List<ResultSet>> cb) {
		CassandraFuture<List<ResultSet>> allResultSetFuture = executeAsync();
		doFutureCallback(allResultSetFuture, cb);
	}

	@Override
	public List<ResultSet> executeNonstop(int timeoutMls) throws TimeoutException {

		List<ResultSet> list = new ArrayList<ResultSet>();

		while (queryIterator.hasNext()) {
			Query query = queryIterator.next();
			ResultSet resultSet = doExecuteNonstop(query, timeoutMls);
			list.add(resultSet);
		}

		return list;
	}

}
