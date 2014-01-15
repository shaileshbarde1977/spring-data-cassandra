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
package org.springdata.cassandra.core;

import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.cql.core.AbstractQueryOperation;
import org.springdata.cassandra.cql.core.CallbackHandler;
import org.springdata.cassandra.cql.core.CassandraFuture;
import org.springdata.cassandra.cql.core.QueryOperation;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Abstract save operation implementation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - entity Type
 * @param <O> - operation Type
 */

public abstract class AbstractSaveOperation<T, O extends QueryOperation<ResultSet, O>> extends
		AbstractQueryOperation<ResultSet, O> {

	protected final CassandraTemplate cassandraTemplate;
	protected final T entity;
	private String tableName;
	private Integer ttl;
	private Long timestamp;

	protected AbstractSaveOperation(CassandraTemplate cassandraTemplate, T entity) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(cassandraTemplate);
		Assert.notNull(entity);
		this.cassandraTemplate = cassandraTemplate;
		this.entity = entity;
	}

	abstract Query createQuery();

	@Override
	public ResultSet execute() {
		Query query = createQuery();
		addSaveOptions(query);
		return doExecute(query);
	}

	@Override
	public CassandraFuture<ResultSet> executeAsync() {
		Query query = createQuery();
		addSaveOptions(query);
		return doExecuteAsync(query);
	}

	@Override
	public void executeAsync(final CallbackHandler<ResultSet> cb) {
		Query query = createQuery();
		addSaveOptions(query);
		doExecuteAsync(query, cb);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		Query query = createQuery();
		addSaveOptions(query);
		return doExecuteNonstop(query, timeoutMls);
	}

	public String getTableName() {
		return tableName != null ? tableName : cassandraTemplate.getTableName(entity.getClass());
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTtl(Integer ttl) {
		this.ttl = ttl;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	private void addSaveOptions(Query query) {

		if (query instanceof Update) {

			Update update = (Update) query;

			/*
			 * Add TTL to Update object
			 */
			if (ttl != null) {
				update.using(QueryBuilder.ttl(ttl));
			}
			if (timestamp != null) {
				update.using(QueryBuilder.timestamp(timestamp));
			}

		} else if (query instanceof Insert) {

			Insert insert = (Insert) query;

			/*
			 * Add TTL to Insert object
			 */
			if (ttl != null) {
				insert.using(QueryBuilder.ttl(ttl));
			}
			if (timestamp != null) {
				insert.using(QueryBuilder.timestamp(timestamp));
			}

		} else {
			throw new IllegalArgumentException("unsupported query object " + query.getClass());
		}

	}

}
