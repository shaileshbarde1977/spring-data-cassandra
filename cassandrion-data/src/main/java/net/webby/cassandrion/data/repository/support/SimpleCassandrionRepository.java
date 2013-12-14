/*
 * Copyright 2013 the original author or authors.
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
package net.webby.cassandrion.data.repository.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.webby.cassandrion.core.CassandrionOperations;
import net.webby.cassandrion.data.core.CassandrionDataOperations;
import net.webby.cassandrion.data.core.CassandrionDataTemplate;
import net.webby.cassandrion.data.repository.CassandrionRepository;
import net.webby.cassandrion.data.repository.query.CassandrionEntityInformation;

import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Repository base implementation for Cassandra.
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleCassandrionRepository<T, ID extends Serializable> implements CassandrionRepository<T, ID> {

	private final CassandrionDataTemplate cassandraDataTemplate;
	private final CassandrionEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleCassandrionRepository} for the given {@link CassandrionEntityInformation} and
	 * {@link CassandrionDataTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public SimpleCassandrionRepository(CassandrionEntityInformation<T, ID> metadata,
			CassandrionDataTemplate cassandraDataTemplate) {

		Assert.notNull(cassandraDataTemplate);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.cassandraDataTemplate = cassandraDataTemplate;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
	 */
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");
		cassandraDataTemplate.saveNew(entity, entityInformation.getTableName());
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	public <S extends T> List<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> result = new ArrayList<S>();

		for (S entity : entities) {
			save(entity);
			result.add(entity);
		}

		return result;
	}

	private Clause getIdClause(ID id) {
		Clause clause = QueryBuilder.eq(entityInformation.getIdColumn(), id);
		return clause;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	public T findOne(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraDataTemplate.findById(id, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.CassandraRepository#findByPartitionKey(java.io.Serializable)
	 */
	@Override
	public List<T> findByPartitionKey(ID id) {
		Assert.notNull(id, "The given id must not be null!");

		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		select.where(getIdClause(id));

		return cassandraDataTemplate.findByQuery(select, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	public boolean exists(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		Select select = QueryBuilder.select().countAll().from(entityInformation.getTableName());
		select.where(getIdClause(id));

		Long num = cassandraDataTemplate.countByQuery(select);
		return num != null && num.longValue() > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	public long count() {
		return cassandraDataTemplate.count(entityInformation.getTableName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	public void delete(ID id) {
		Assert.notNull(id, "The given id must not be null!");

		cassandraDataTemplate.deleteById(false, id, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	public void delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		delete(entityInformation.getId(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	public void delete(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		for (T entity : entities) {
			delete(entity);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteAll()
	 */
	public void deleteAll() {
		cassandraDataTemplate.truncate(entityInformation.getTableName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	public List<T> findAll() {
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		return findAll(select);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
	 */
	public Iterable<T> findAll(Iterable<ID> ids) {

		List<ID> parameters = new ArrayList<ID>();
		for (ID id : ids) {
			parameters.add(id);
		}
		Clause clause = QueryBuilder.in(entityInformation.getIdColumn(), parameters.toArray());
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		select.where(clause);

		return findAll(select);
	}

	private List<T> findAll(Select query) {

		if (query == null) {
			return Collections.emptyList();
		}

		return cassandraDataTemplate.findByQuery(query, entityInformation.getJavaType());
	}

	/**
	 * Returns the underlying {@link CassandrionOperations} instance.
	 * 
	 * @return
	 */
	protected CassandrionOperations getCassandraOperations() {
		return this.cassandraDataTemplate;
	}

	/**
	 * Returns the underlying {@link CassandrionDataOperations} instance.
	 * 
	 * @return
	 */
	protected CassandrionDataOperations getCassandraDataOperations() {
		return this.cassandraDataTemplate;
	}

	/**
	 * @return the entityInformation
	 */
	protected CassandrionEntityInformation<T, ID> getEntityInformation() {
		return entityInformation;
	}

}
