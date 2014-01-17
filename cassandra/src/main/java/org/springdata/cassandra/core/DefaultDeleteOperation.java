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

import java.util.List;

import org.springdata.cassandra.cql.core.AbstractUpdateOperation;
import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Implementation of the DeleteOperation.
 * 
 * Supports two types of deletion operations: by Id and by Entity.
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultDeleteOperation<T> extends AbstractUpdateOperation<DeleteOperation> implements DeleteOperation,
		BatchedStatementCreator {

	enum DeleteBy {
		ID, ENTITY;
	}

	private final CassandraTemplate cassandraTemplate;
	private final DeleteBy deleteBy;
	private final T entity;
	private final Class<T> entityClass;
	private final Object id;

	private String tableName;
	private Long timestamp;

	@SuppressWarnings("unchecked")
	protected DefaultDeleteOperation(CassandraTemplate cassandraTemplate, T entity) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(entity);
		this.cassandraTemplate = cassandraTemplate;
		this.deleteBy = DeleteBy.ENTITY;
		this.entity = entity;
		this.entityClass = (Class<T>) entity.getClass();
		this.id = null;
	}

	protected DefaultDeleteOperation(CassandraTemplate cassandraTemplate, Class<T> entityClass, Object id) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(entityClass);
		Assert.notNull(id);
		this.cassandraTemplate = cassandraTemplate;
		this.deleteBy = DeleteBy.ID;
		this.entity = null;
		this.entityClass = entityClass;
		this.id = id;
	}

	@Override
	public DeleteOperation fromTable(String tableName) {
		this.tableName = tableName;
		return this;
	}

	@Override
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public DeleteOperation withTimestamp(long timestampMls) {
		this.timestamp = timestampMls;
		return this;
	}

	private String getTableName() {
		if (tableName != null) {
			return tableName;
		}

		switch (deleteBy) {
		case ID:
			return cassandraTemplate.getTableName(entityClass);
		case ENTITY:
			return cassandraTemplate.getTableName(entity.getClass());
		}

		throw new IllegalArgumentException("invalid delete type " + deleteBy);
	}

	@Override
	public Query createQuery() {
		return createStatement();
	}

	@Override
	public Statement createStatement() {

		Delete.Selection ds = QueryBuilder.delete();

		Delete query = ds.from(cassandraTemplate.getKeyspace(), getTableName());
		Where w = query.where();

		switch (deleteBy) {

		case ID:
			CassandraPersistentEntity<?> persistenceEntity = cassandraTemplate.getPersistentEntity(entityClass);
			List<Clause> list = cassandraTemplate.getConverter().getPrimaryKey(persistenceEntity, id);

			for (Clause c : list) {
				w.and(c);
			}

			break;

		case ENTITY:
			cassandraTemplate.getConverter().write(entity, w);
			break;

		}

		if (timestamp != null) {
			query.using(QueryBuilder.timestamp(timestamp));
		}

		return query;
	}

}
